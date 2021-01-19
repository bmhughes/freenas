from middlewared.service import Service, CallError, accepts, private, job
from middlewared.plugins.cluster_linux.utils import CTDBConfig
from middlewared.plugins.gluster_linux.utils import run_method
from glustercli.cli import volume

import os
import pathlib
import subprocess


MOUNT_UMOUNT_LOCK = CTDBConfig.MOUNT_UMOUNT_LOCK.value
CRE_OR_DEL_LOCK = CTDBConfig.CRE_OR_DEL_LOCK.value
CTDB_VOL_NAME = CTDBConfig.CTDB_VOL_NAME.value
CTDB_LOCAL_MOUNT = CTDBConfig.CTDB_LOCAL_MOUNT.value


class CtdbSharedVolumeService(Service):

    class Config:
        namespace = 'ctdb.shared.volume'
        cli_namespace = 'sharing.ctdb.volume'

    @private
    def construct_gluster_volume_create_data(self, peers):

        payload = {}

        # get the system dataset location
        ctdb_sysds_path = self.middleware.call_sync('systemdataset.config')['path']
        ctdb_sysds_path = os.path.join(ctdb_sysds_path, CTDB_VOL_NAME)

        bricks = []
        for i in peers:
            peer = i
            path = ctdb_sysds_path
            brick = peer + ':' + path
            bricks.append(brick)

        payload = {
            'bricks': bricks,
            'replica': len(peers),
            'force': True,
        }

        return payload

    @private
    def shared_volume_exists_and_started(self):

        exists = started = False

        vol = run_method(volume.status_detail, volname=CTDB_VOL_NAME)
        if vol:
            if vol[0]['type'] != 'REPLICATE':
                raise CallError(
                    f'A volume with the name "{CTDB_VOL_NAME}" already exists '
                    'but is not a REPLICATE type volume. Please delete or rename '
                    'this volume and try again.'
                )
            elif vol[0]['replica'] < 3 or vol[0]['num_bricks'] < 3:
                raise CallError(
                    f'A volume with the name "{CTDB_VOL_NAME}" already exists '
                    'but is configured in a way that could cause split-brain. '
                    'Please delete or rename this volume and try again.'
                )
            elif vol[0]['status'] != 'Started':
                exists = True
                run_method(volume.start, CTDB_VOL_NAME)
                started = True
            else:
                exists = started = True

        return exists, started

    @accepts()
    @job(lock=CRE_OR_DEL_LOCK)
    def create(self, job):
        """
        Create and mount the shared volume to be used
        by ctdb daemon.
        """

        # check if ctdb shared volume already exists and started
        exists, started = self.shared_volume_exists_and_started()

        if not exists:
            # get the peers in the TSP
            peers = self.middleware.call_sync('gluster.peer.pool')
            if not peers:
                raise CallError('No peers detected')

            # shared storage volume requires 3 nodes, minimally, to
            # prevent the dreaded split-brain
            con_peers = [i['hostname'] for i in peers if i['connected'] == 'Connected']
            if len(con_peers) < 3:
                raise CallError(
                    '3 peers must be present and connected before the ctdb '
                    'shared volume can be created.'
                )

            # create the ctdb shared volume
            req = self.construct_gluster_volume_create_data(con_peers)
            run_method(volume.create, CTDB_VOL_NAME, req.pop('bricks'), **req)

        if not started:
            # start it if we get here
            run_method(volume.start, CTDB_VOL_NAME)

        # try to mount it locally
        mount_job = self.middleware.call_sync('ctdb.shared.volume.mount')
        mount_job.wait_sync(raise_error=True)

        return 'SUCCESS'

    @accepts()
    @job(lock=CRE_OR_DEL_LOCK)
    def delete(self, job):
        """
        Delete and unmount the shared volume used by ctdb daemon.
        """

        # nothing to delete if it doesn't exist
        exists, started = self.shared_volume_exists_and_started()
        if not exists:
            return

        # umount it first
        umount_job = self.middleware.call_sync('ctdb.shared.volume.umount')
        umount_job.wait_sync(raise_error=True)

        if started:
            # stop the volume
            force = {'force': True}
            run_method(volume.stop, CTDB_VOL_NAME, **force)

        # now delete it
        run_method(volume.delete, CTDB_VOL_NAME)

        return 'SUCCESS'

    @accepts()
    @job(lock=MOUNT_UMOUNT_LOCK)
    def mount(self, job):
        """
        Mount the ctdb shared volume locally.
        """

        mounted = False

        # if you try to mount without the service being started,
        # the mount utility simply returns a msg to stderr stating
        # "Mounting glusterfs on /cluster/ctdb_shared_vol failed" which is
        # expected since the service isn't running
        if not self.middleware.call_sync('service.started', 'glusterd'):
            self.logger.warning('The "glusterd" service is not running. Not mounting.')
            return mounted

        exists, started = self.shared_volume_exists_and_started()
        if not exists or not started:
            return mounted

        try:
            # make sure the dirs are there
            pathlib.Path(CTDB_LOCAL_MOUNT).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise CallError(f'Failed creating directory with error: {e}')

        # we have to stop the glustereventsd service because when the volume is mounted
        # it triggers an event which then gets processed by middlewared and calls this
        # method again....
        # Furthermore, there is a scenario which can cause a mount/umount loop. If the
        # shared volume is in a self-heal situation because, say, 1 of the peers is down.
        # when we `mount -t glusterfs`, 2 events get generated with identical timestamps.
        # There is nothing we can do about the events and since the timestamps are in
        # seconds (which means they appear at the "same" time), then you can get into a
        # really bad loop. In the above scenario, when mounting a AFR_SUBVOLS_DOWN event
        # is generated and an AFR_SUBVOL_UP event is generated and they both have the same
        # timestamp. Well, according to testing, the SUBVOLS_DOWN event comes "first"
        # while the SUBVOL_UP event comes "second". This means on SUBVOLS_DOWN event, we
        # call the `ctdb.shared.volume.umount` method which generates another `SUBVOLS_DOWN`
        # method and because we processed a `SUBVOL_UP` event, we run this method which
        # generates another SUBVOL_UP event........This causes a mount/umount loop.
        # So, as an easy work-around we stop the glusterevevntsd service which prevents
        # any more events from being triggered while we mount/umount the volume.
        self.middleware.call_sync('service.stop', 'glustereventsd')
        try:
            cmd = ['mount', '-t', 'glusterfs', 'localhost:/' + CTDB_VOL_NAME, CTDB_LOCAL_MOUNT]
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if cp.returncode:
                if b'is already mounted' in cp.stderr:
                    mounted = True
                else:
                    errmsg = cp.stderr.decode().strip()
                    self.logger.error(f'Failed to mount {CTDB_LOCAL_MOUNT} with error: {errmsg}')
            else:
                mounted = True
        except Exception:
            self.logger.error(
                'Unhandled exception when trying to mount ctdb shared volume', exc_info=True
            )
        finally:
            self.middleware.call_sync('service.start', 'glustereventsd')

        return mounted

    @accepts()
    @job(lock=MOUNT_UMOUNT_LOCK)
    def umount(self, job):
        """
        Unmount the locally mounted ctdb shared volume.
        """

        umounted = False

        # read the above comment in the `def mount` method on why we stop and
        # start this service before we umount the volume
        self.middleware.call_sync('service.stop', 'glustereventsd')
        try:
            cmd = ['umount', '-R', CTDB_LOCAL_MOUNT]
            cp = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if cp.returncode:
                errmsg = cp.stderr.decode().strip()
                if 'not mounted' in errmsg or 'ctdb_shared_vol: not found' in errmsg:
                    umounted = True
                else:
                    self.logger.error(f'Failed to umount {CTDB_LOCAL_MOUNT} with error: {errmsg}')
            else:
                umounted = True
        except Exception:
            self.logger.error(
                'Unhandled exception when trying to umount ctdb shared volume', exc_info=True
            )
        finally:
            self.middleware.call_sync('service.start', 'glustereventsd')

        return umounted
