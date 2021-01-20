from middlewared.schema import Dict, IPAddr, Int, Str
from middlewared.service import (accepts, private, job,
                                 CRUDService, ValidationErrors,
                                 CallError, filterable)
from middlewared.utils import filter_list
from middlewared.plugins.cluster_linux.utils import CTDBConfig
import pathlib


SHARED_VOL = CTDBConfig.CTDB_LOCAL_MOUNT.value
PUB_IP_FILE = CTDBConfig.GM_PUB_IP_FILE.value
ETC_IP_FILE = CTDBConfig.ETC_PUB_IP_FILE.value
PUB_LOCK = CTDBConfig.PUB_LOCK.value


class CtdbIpService(CRUDService):

    class Config:
        namespace = 'ctdb.public.ips'

    @filterable
    def query(self, filters=None, options=None):

        ips = []
        # logic is as follows:
        #   1. if ctdb daemon is started
        #       ctdb just reads the `ETC_IP_FILE` and loads the
        #       ips written there into the cluster. However,
        #       if a public ip is added/removed, it doesn't
        #       mean the ctdb cluster has been reloaded to
        #       see the changes in the file. So return what
        #       the daemon sees.
        #   2. if the `SHARED_VOL` is mounted and the `ETC_IP_FILE` exists
        #       and is a symlink and the symlink is pointed to the
        #       `PUB_IP_FILE` then read it and return the contents
        if self.middleware.call_sync('service.started', 'ctdb'):
            ips = self.middleware.call_sync('ctdb.general.ips')
            ips = list(map(lambda i: dict(i, id=i['public_ip']), ips))
        else:
            try:
                mounted = pathlib.Path(SHARED_VOL).is_mount()
            except Exception:
                # can happen when mounted but glusterd service
                # is stopped/crashed etc
                mounted = False

            if mounted:
                pub_ip_file = pathlib.Path(PUB_IP_FILE)
                etc_ip_file = pathlib.Path(ETC_IP_FILE)
                if pub_ip_file.exists():
                    if etc_ip_file.is_symlink() and etc_ip_file.resolve() == pub_ip_file:
                        with open(PUB_IP_FILE) as f:
                            lines = f.read().splitlines()
                            for i in lines:
                                ips.append({
                                    'id': i.split('/')[0],
                                    'public_ip': i.split('/')[0],
                                    'netmask': int(i.split('/')[1].split()[0]),
                                    'interfaces': list(i.split()[-1]),
                                })

        return filter_list(ips, filters, options)

    @private
    async def common_validation(self, data, schema_name, verrors, delete=False):

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

        # we have to make sure the IP that was given to us doesn't exist
        # on the node since ctdb daemon automatically creates and manages
        # this address
        node_ips = [i['address'] for i in (await self.middleware.call('interface.ip_in_use', {'static': True}))]
        if data['ip'] in node_ips:
            verrors.add(
                f'{schema_name}.{data["ip"]}',
                f'{data["ip"]} already exist on this node. ',
            )

        verrors.check()

        # get the current ips in the cluster
        cur_ips = [i['id'] for i in (await self.middleware.call('ctdb.private.ips.query'))]
        cur_ips.extend([i['id'] for i in (await self.middleware.call('ctdb.public.ips.query'))])

        if not delete:
            # validate the netmask
            netmask_min = 0
            v4max = 32
            v6max = 128
            bad_netmask = False
            if ':' in data['ip'] and data['netmask'] > v6max:
                bad_netmask = True
            elif data['netmask'] > v4max:
                bad_netmask = True
            elif data['netmask'] < netmask_min:
                bad_netmask = True

            if bad_netmask:
                verrors.add(
                    f'{schema_name}.{data["netmask"]}',
                    f'The netmask: "{data["netmask"]}" for "{data["ip"]}" is invalid.',
                )

            # validate the interface
            node_ints = [i['id'] for i in (await self.middleware.call('interface.query'))]
            if data['interface'] not in node_ints:
                verrors.add(
                    f'{schema_name}.{data["interface"]}',
                    f'{data["interface"]} does not exist on this node. ',
                )

            # make sure public ip doesn't already exist in the cluster
            if data['ip'] in cur_ips:
                verrors.add(
                    f'{schema_name}.{data["ip"]}',
                    f'"{data["ip"]}" already in the cluster.'
                )
        else:
            # make sure public ip exists before deleting from the cluster
            if data['ip'] not in cur_ips:
                verrors.add(
                    f'{schema_name}.{data["ip"]}',
                    f'"{data["ip"]}" does not exist in the cluster.'
                )

        verrors.check()

    @private
    def update_file(self, data, verrors, delete=False):
        """
        Update the ctdb cluster public IP file.
        """

        ctdb_file = pathlib.Path(CTDBConfig.GM_PUB_IP_FILE.value)
        etc_file = pathlib.Path(CTDBConfig.ETC_PUB_IP_FILE.value)

        # make sure the public ip file exists
        try:
            ctdb_file.touch(exist_ok=True)
        except Exception as e:
            raise CallError(f'Failed creating {ctdb_file} with error {e}')

        # we need to make sure that the local etc public ip file
        # is symlinked to the ctdb shared volume public ip file
        symlink_it = delete_it = False
        if etc_file.exists():
            if not etc_file.is_symlink():
                # delete it since we're symlinking it
                delete_it = True
            else:
                # means it's a symlink but not to the ctdb
                # shared volume public ip file
                if not etc_file.resolve() == ctdb_file:
                    delete_it = True
        else:
            symlink_it = True

        if delete_it:
            try:
                etc_file.unlink()
            except Exception as e:
                raise CallError(f'Failed deleting {etc_file} with errror {e}')

        if symlink_it:
            try:
                etc_file.symlink_to(ctdb_file)
            except Exception as e:
                raise CallError(f'Failed symlinking {etc_file} to {ctdb_file} with error {e}')

        if not delete:
            entry = f'{data["ip"]}/{data["netmask"]} {data["interface"]}'
            with open(PUB_IP_FILE, 'a') as f:
                f.write(entry + '\n')
        else:
            with open(PUB_IP_FILE) as f:
                lines = f.read().splitlines()

            lines.remove(next(i for i in lines if data['ip'] in i))
            with open(PUB_IP_FILE, 'w') as f:
                f.writelines(map(lambda x: x + '\n', lines))

    @accepts(Dict(
        'node_create',
        IPAddr('ip', required=True),
        Int('netmask', required=True),
        Str('interface', required=True),
    ))
    @job(lock=PUB_LOCK)
    async def do_create(self, job, data):
        """
        Add a ctdb public address to the cluster

        `ip` is an IP v4/v6 address
        `netmask` is a cidr notated netmask (i.e. 16/24/48/64 etc)
        `interface` a network interface to have assigned the `ip`
        """

        schema_name = 'node_create'
        verrors = ValidationErrors()

        await self.middleware.call('ctdb.public.ips.common_validation', data, schema_name, verrors)
        await self.middleware.call('ctdb.public.ips.update_file', data, verrors)

        return data

    @accepts(Str('id'))
    @job(lock=PUB_LOCK)
    async def do_delete(self, job, id):
        """
        Delete a Public IP address from the ctdb cluster.

        `ip` is an IP v4/v6 address
        """

        schema_name = 'node_delete'
        verrors = ValidationErrors()

        data = {'ip': (await self.get_instance(id))['id']}
        await self.middleware.call('ctdb.public.ips.common_validation', data, schema_name, verrors, True)
        await self.middleware.call('ctdb.public.ips.update_file', data, verrors, True)

        return data['ip']
