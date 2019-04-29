from middlewared.schema import accepts, Bool, Cron, Dict, Int, Patch, Str
from middlewared.service import CRUDService, job, private, ValidationErrors
from middlewared.validators import Range
from middlewared.utils import run_command_with_user_context

import subprocess


class CronJobService(CRUDService):

    class Config:
        datastore = 'tasks.cronjob'
        datastore_prefix = 'cron_'
        datastore_extend = 'cronjob.cron_extend'
        namespace = 'cronjob'

    @private
    def cron_extend(self, data):
        Cron.convert_db_format_to_schedule(data)
        return data

    @private
    async def construct_cron_command(self, schedule, user, command, stdout=True, stderr=True):
        return list(
            filter(
                bool, (
                    schedule['minute'], schedule['hour'], schedule['dom'], schedule['month'],
                    schedule['dow'], user,
                    'PATH="/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:/root/bin"',
                    command.replace('\n', '').replace('%', r'\%'),
                    '> /dev/null' if stdout else '', '2> /dev/null' if stderr else ''
                )
            )
        )

    @private
    async def validate_data(self, data, schema):
        verrors = ValidationErrors()

        user = data.get('user')
        if user:
            # Windows users can have spaces in their usernames
            # http://www.freebsd.org/cgi/query-pr.cgi?pr=164808
            if ' ' in user:
                verrors.add(
                    f'{schema}.user',
                    'Usernames cannot have spaces'
                )

            elif not (
                await self.middleware.call(
                    'notifier.get_user_object',
                    user
                )
            ):
                verrors.add(
                    f'{schema}.user',
                    'Specified user does not exist'
                )

        command = data.get('command')
        if not command:
            verrors.add(
                f'{schema}.command',
                'Please specify a command for cronjob task.'
            )
        else:
            crontab_cmd = (await self.construct_cron_command(
                data['schedule'], user, command, data['stdout'], data['stderr']
            ))[6:]
            command = crontab_cmd.pop(1)

            # When cron(8) reads an entry from a crontab, it keeps a buffer of 1000 characters and anything more then
            # that is truncated. We validate that the user supplied command is not more than 1000 characters.
            allowed_length = 1000 - len(' '.join(crontab_cmd))

            if len(command) > allowed_length:
                verrors.add(
                    f'{schema}.command',
                    f'Command must be less than or equal to {allowed_length} characters. '
                    'Newline characters are automatically removed and "%" characters escaped with a backslash.'
                )

        return verrors, data

    @accepts(
        Dict(
            'cron_job_create',
            Bool('enabled'),
            Bool('stderr', default=False),
            Bool('stdout', default=True),
            Cron(
                'schedule',
                defaults={'minute': '00'}
            ),
            Str('command', required=True),
            Str('description'),
            Str('user', required=True),
            register=True
        )
    )
    async def do_create(self, data):
        """
        Create a new cron job.

        `stderr` and `stdout` are boolean values which if `true`, represent that we would like to suppress
        standard error / standard output respectively.

        .. examples(websocket)::

          Create a cron job which executes `touch /tmp/testfile` after every 5 minutes.

            :::javascript
            {
                "id": "6841f242-840a-11e6-a437-00e04d680384",
                "msg": "method",
                "method": "cronjob.create",
                "params": [{
                    "enabled": true,
                    "schedule": {
                        "minute": "5",
                        "hour": "*",
                        "dom": "*",
                        "month": "*",
                        "dow": "*"
                    },
                    "command": "touch /tmp/testfile",
                    "description": "Test command",
                    "user": "root",
                    "stderr": true,
                    "stdout": true
                }]
            }
        """
        verrors, data = await self.validate_data(data, 'cron_job_create')
        if verrors:
            raise verrors

        Cron.convert_schedule_to_db_format(data)

        data['id'] = await self.middleware.call(
            'datastore.insert',
            self._config.datastore,
            data,
            {'prefix': self._config.datastore_prefix}
        )

        await self.middleware.call('service.restart', 'cron')

        return await self._get_instance(data['id'])

    @accepts(
        Int('id', validators=[Range(min=1)]),
        Patch('cron_job_create', 'cron_job_update', ('attr', {'update': True}))
    )
    async def do_update(self, id, data):
        """
        Update cronjob of `id`.
        """
        task_data = await self.query(filters=[('id', '=', id)], options={'get': True})
        original_data = task_data.copy()
        task_data.update(data)
        verrors, task_data = await self.validate_data(task_data, 'cron_job_update')

        if verrors:
            raise verrors

        Cron.convert_schedule_to_db_format(task_data)
        Cron.convert_schedule_to_db_format(original_data)

        if len(set(task_data.items()) ^ set(original_data.items())) > 0:

            await self.middleware.call(
                'datastore.update',
                self._config.datastore,
                id,
                task_data,
                {'prefix': self._config.datastore_prefix}
            )

            await self.middleware.call('service.restart', 'cron')

        return await self._get_instance(id)

    @accepts(
        Int('id')
    )
    async def do_delete(self, id):
        """
        Delete cronjob of `id`.
        """
        response = await self.middleware.call(
            'datastore.delete',
            self._config.datastore,
            id
        )

        await self.middleware.call('service.restart', 'cron')

        return response

    @accepts(
        Int('id')
    )
    @job(lock=lambda args: args[-1], logs=True)
    def run(self, job, id):
        """
        Job to run cronjob task of `id`.
        """
        cron_task = self.middleware.call_sync('cronjob._get_instance', id)
        cron_cmd = ' '.join(
            self.middleware.call_sync(
                'cronjob.construct_cron_command', cron_task['schedule'], cron_task['user'],
                cron_task['command'], cron_task['stdout'], cron_task['stderr']
            )[7:]
        )

        job.set_progress(
            10,
            'Executing Cron Task'
        )

        cp = run_command_with_user_context(
            cron_cmd, cron_task['user'], 'cron', lambda v: job.logs_fd.write(v)
        )

        job.set_progress(
            85,
            'Executed Cron Task'
        )

        if cp.stdout:
            email = (
                self.middleware.call_sync('user.query', [['username', '=', cron_task['user']]], {'get': True})
            )['email']
            stdout = cp.stdout.decode()
            if email:
                mail_job = self.middleware.call_sync(
                    'mail.send', {
                        'subject': 'CronTask Manual Run',
                        'text': stdout,
                        'to': [email]
                    }
                )

                job.set_progress(
                    95,
                    'Sending mail for Cron Task output'
                )

                mail_job.wait_sync()
                if mail_job.error:
                    job.logs_fd.write(f'Failed to send email for CronTask manual run: {mail_job.error}'.encode())
            else:
                job.set_progress(
                    95,
                    'Email for root user not configured. Skipping sending mail.'
                )

            job.logs_fd.write(f'Executed CronTask - {cron_cmd}: {stdout}'.encode())

        job.set_progress(
            100,
            'Manual run of Cron Task complete.'
        )
