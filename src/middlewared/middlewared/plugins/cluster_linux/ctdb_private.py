from middlewared.schema import Dict, IPAddr
from middlewared.service import (accepts, private, job,
                                 Service, ValidationErrors,
                                 CallError)
from middlewared.plugins.cluster_linux.utils import CTDBConfig
import pathlib


SHARED_VOL = CTDBConfig.CTDB_LOCAL_MOUNT.value
PRI_IP_FILE = CTDBConfig.GM_PRI_IP_FILE.value
ETC_IP_FILE = CTDBConfig.ETC_PRI_IP_FILE.value
PRI_LOCK = CTDBConfig.PRI_LOCK.value


class CtdbIpService(Service):

    class Config:
        namespace = 'ctdb.private.ips'

    @private
    def query(self):

        ips = []

        # logic is as follows:
        #   1. if ctdb daemon is started
        #       ctdb just reads the `ETC_IP_FILE` and loads the
        #       ips written there into the cluster. However,
        #       if a private ip is added/removed, it doesn't
        #       mean the ctdb cluster has been reloaded to
        #       see the changes in the file. So return what
        #       the daemon sees.
        #   2. else
        #       if the `SHARED_VOL` is mounted and `ETC_IP_FILE` exists
        #       and is a symlink and the symlink is pointed to the
        #       `PRI_IP_FILE` then read it and return the contents
        if self.middleware.call_sync('service.started', 'ctdb'):
            ips = [i['address'] for i in self.middleware.call_sync('ctdb.general.listnodes')]
        else:
            try:
                mounted = pathlib.Path(SHARED_VOL).is_mount()
            except Exception:
                # can happen when mounted but glusterd service
                # is stopped/crashed etc
                mounted = False

            if mounted:
                pri_ip_file = pathlib.Path(PRI_IP_FILE)
                etc_ip_file = pathlib.Path(ETC_IP_FILE)
                if pri_ip_file.exists():
                    if etc_ip_file.is_symlink() and etc_ip_file.resolve() == pri_ip_file:
                        with open(PRI_IP_FILE) as f:
                            ips = f.read().splitlines()
        return ips

    @private
    async def common_validation(self, pri_ip, schema_name, verrors, delete=False):

        # make sure that the cluster shared volume is mounted
        if not await self.middleware.call('service.started', 'glusterd'):
            verrors.add(
                f'{schema_name}.glusterd',
                'The "glusterd" service is not started.',
            )

        try:
            mounted = pathlib.Path(SHARED_VOL).is_mount()
        except Exception:
            mounted = False

        if not mounted:
            verrors.add(
                f'{schema_name}.{SHARED_VOL}',
                f'"{SHARED_VOL}" is not mounted'
            )

        verrors.check()

        # get the current ips in the cluster
        cur_ips = await self.middleware.call('ctdb.private.ips.query')
        cur_ips.extend((await self.middleware.call('ctdb.public.ips.query')))

        if not delete:
            # make sure private ip doesn't already exist in the cluster
            if pri_ip in cur_ips:
                verrors.add(
                    f'{schema_name}.{pri_ip}',
                    f'"{pri_ip}" already in the cluster.'
                )
        else:
            # make sure private ip exists before deleting from the cluster
            if pri_ip not in cur_ips:
                verrors.add(
                    f'{schema_name}.{pri_ip}',
                    f'"{pri_ip}" does not exist in the cluster.'
                )

        verrors.check()

    @private
    def update_file(self, pri_ip, verrors, delete=False):
        """
        Update the ctdb cluster private IP file.
        """

        # ctdb documentation is _VERY_ explicit in
        # how the private IP file is modified

        # the documentation clearly states that before
        # adding a private peer, the cluster must be
        # healthy. This requires running a command that
        # is expecting the ctdb daemon to be started.
        # If this is the first private peer being added
        # then the ctdb daemon isn't going to be started
        # which means we can't check if the cluster is
        # healthy. So we do the following:
        #   1. if the ctdb shared volume private ip file exists
        #       then assume that this isn't the first peer
        #       being added to the cluster and check the ctdb
        #       daemon for the cluster health.
        #   2. elif the ctdb shared volume private ip doesnt
        #       exist then assume this is the first peer being
        #       added to the cluster and skip the cluster health
        #       check.
        ctdb_file = pathlib.Path(CTDBConfig.GM_PRI_IP_FILE.value)
        etc_file = pathlib.Path(CTDBConfig.ETC_PRI_IP_FILE.value)

        if ctdb_file.exists():
            if self.middleware.call_sync('service.started', 'ctdb'):
                if not self.middleware.call_sync('ctdb.general.healthy'):
                    self.logger.error('ctdb cluster is not healthy')
                    return False

        # make sure the private ip file exists
        try:
            ctdb_file.touch(exist_ok=True)
        except Exception:
            self.logger.error('Failed creating %s', str(ctdb_file), exc_info=True)
            return False

        # we need to make sure that the local etc private ip file
        # is symlinked to the ctdb shared volume private ip file
        symlink_it = delete_it = False
        if etc_file.exists():
            if not etc_file.is_symlink():
                # delete it since we're symlinking it
                delete_it = True
            else:
                # means it's a symlink but not to the ctdb
                # shared volume private ip file
                if not etc_file.resolve() == ctdb_file:
                    delete_it = True
        else:
            symlink_it = True

        if delete_it:
            try:
                etc_file.unlink()
            except Exception:
                self.logger.error('Failed deleting %s', str(etc_file), exc_info=True)
                return False

        if symlink_it:
            try:
                etc_file.symlink_to(ctdb_file)
            except Exception:
                self.logger.error('Failed symlinking %s to %s', str(etc_file), str(ctdb_file), exc_info=True)
                return False

        # ctdb documentation is _VERY_ explicit about
        # how this file should be modified
        if not delete:
            # in the case of adding a node, it _MUST_ be
            # added to the end of the file always
            with open(PRI_IP_FILE, 'a') as f:
                f.write(pri_ip + '\n')
                f.flush()
        else:
            with open(PRI_IP_FILE) as f:
                lines = f.read().splitlines()

            # in the case of removing a node, a "#"
            # _MUST_ be put in front of the entry in
            # the file and the other lines _MUST_ be
            # kept in the same order when writing it
            # back out or ctdb will barf and fail to
            # reload...
            index = lines.index(pri_ip)
            lines[index] = '#' + pri_ip

            with open(PRI_IP_FILE, 'w') as f:
                f.writelines(map(lambda x: x + '\n', lines))
                f.flush()

        return True

    @accepts(Dict(
        'node_create',
        IPAddr('ip'),
    ))
    @job(lock=PRI_LOCK)
    async def create(self, job, data):
        """
        Add a ctdb private address to the cluster

        `ip` is an IP v4/v6 address
        """

        schema_name = 'node_create'
        verrors = ValidationErrors()

        await self.middleware.call(
            'ctdb.private.ips.common_validation', data['ip'], schema_name, verrors
        )
        if not await self.middleware.call('ctdb.private.ips.update_file', data['ip'], verrors):
            raise CallError(f'Failed updating {PRI_IP_FILE}. Please check logs.')

        return data['ip']

    @accepts(Dict(
        'node_delete',
        IPAddr('ip'),
    ))
    @job(lock=PRI_LOCK)
    async def delete(self, job, data):
        """
        Delete a Private IP address from the ctdb cluster.

        `ip` is an IP v4/v6 address
        """

        schema_name = 'node_delete'
        verrors = ValidationErrors()

        await self.middleware.call(
            'ctdb.private.ips.common_validation', data['ip'], schema_name, verrors, True
        )
        if not await self.middleware.call('ctdb.private.ips.update_file', data['ip'], verrors, True):
            raise CallError(f'Failed updating {PRI_IP_FILE}. Please check logs.')

        return data['ip']
