import collections
import copy
import enum
import os
import shutil
import tempfile
import yaml

from middlewared.schema import accepts, Dict, Str
from middlewared.service import CallError, CRUDService, filterable, filter_list, private

from .utils import CHART_NAMESPACE, run


class Resources(enum.Enum):
    DEPLOYMENTS = 'deployments'
    PODS = 'pods'


class ChartReleaseService(CRUDService):

    class Config:
        namespace = 'chart.release'

    @filterable
    async def query(self, filters=None, options=None):
        if not await self.middleware.call('service.started', 'kubernetes'):
            return []

        options = options or {}
        extra = copy.deepcopy(options.get('extra', {}))
        get_resources = extra.get('retrieve_resources')
        if get_resources:
            resources = {r.name: collections.defaultdict(list) for r in Resources}
            for resource, namespace, r_filters, n_func in (
                (
                    Resources.DEPLOYMENTS, 'k8s.deployment', [
                        ['metadata.labels.app\\.kubernetes\\.io/managed-by', '=', 'Helm'],
                    ], lambda r: r['metadata']['labels']['app.kubernetes.io/instance']
                ),
                (
                    Resources.PODS, 'k8s.pod', [['metadata.labels.app\\.kubernetes\\.io/instance', '!=', None]],
                    lambda r: r['metadata']['labels']['app.kubernetes.io/instance']
                ),
            ):
                r_filters += [['metadata.namespace', '=', CHART_NAMESPACE]]
                for r_data in await self.middleware.call(f'{namespace}.query', r_filters):
                    resources[resource.name][n_func(r_data)].append(r_data)

        release_secrets = await self.middleware.call('chart.release.releases_secrets')
        releases = []
        for name, release in release_secrets.items():
            release_data = release['releases'].pop(0)
            release_data.update({
                'history': release['releases'],
            })
            if get_resources:
                release_data['resources'] = {r.name: resources[r.name][name] for r in Resources}

            releases.append(release_data)

        return filter_list(releases, filters, options)

    @accepts(
        Dict(
            'chart_release_create',
            Dict('values', additional_attrs=True),
            Str('catalog', required=True),
            Str('item', required=True),
            Str('release_name', required=True),
            Str('train', default='charts'),
            Str('version', required=True),
        )
    )
    async def do_create(self, data):
        await self.middleware.call('kubernetes.validate_k8s_setup')
        if await self.middleware.call('chart.release.query', [['id', '=', data['release_name']]]):
            raise CallError(f'Chart release with {data["release_name"]} already exists.')

        catalog = await self.middleware.call(
            'catalog.query', [['id', '=', data['catalog']]], {'get': True, 'extra': {'item_details': True}}
        )
        if data['train'] not in catalog['trains']:
            raise CallError(f'Unable to locate "{data["train"]}" catalog train.')
        if data['item'] not in catalog['trains'][data['train']]:
            raise CallError(f'Unable to locate "{data["item"]}" catalog item.')
        if data['version'] not in catalog['trains'][data['train']][data['item']]['versions']:
            raise CallError(f'Unable to locate "{data["version"]}" catalog item version.')

        item_details = catalog['trains'][data['train']][data['item']]['versions'][data['version']]
        # The idea is to validate the values provided first and if it passes our validation test, we
        # can move forward with setting up the datasets and installing the catalog item
        default_values = item_details['values']
        new_values = copy.deepcopy(default_values)
        new_values.update(data['values'])
        await self.middleware.call('chart.release.validate_values', item_details, new_values)
        # TODO: Validate if the release name has not been already used, let's do that once we have query
        # in place

        # Now that we have completed validation for the item in question wrt values provided,
        # we will now perform the following steps
        # 1) Create release datasets
        # 2) Copy chart version into release/charts dataset
        # 3) Install the helm chart
        # 4) Create storage class
        k8s_config = await self.middleware.call('kubernetes.config')
        release_ds = os.path.join(k8s_config['dataset'], 'releases', data['release_name'])
        try:
            for dataset in await self.release_datasets(release_ds):
                if not await self.middleware.call('pool.dataset.query', [['id', '=', dataset]]):
                    await self.middleware.call('pool.dataset.create', {'name': dataset, 'type': 'FILESYSTEM'})

            chart_path = os.path.join('/mnt', release_ds, 'charts', data['version'])
            await self.middleware.run_in_thread(lambda: shutil.copytree(item_details['location'], chart_path))

            with tempfile.NamedTemporaryFile(mode='w+') as f:
                f.write(yaml.dump(new_values))
                f.flush()
                # We will install the chart now and force the installation in an ix based namespace
                # https://github.com/helm/helm/issues/5465#issuecomment-473942223
                cp = await run(
                    [
                        'helm', 'install', data['release_name'], chart_path, '-n',
                        CHART_NAMESPACE, '--create-namespace', '-f', f.name,
                    ],
                    check=False,
                )
            if cp.returncode:
                raise CallError(f'Failed to install catalog item: {cp.stderr}')

            storage_class = await self.middleware.call('k8s.storage_class.retrieve_storage_class_manifest')
            storage_class_name = f'ix-storage_class-{data["release_name"]}'
            storage_class['metadata']['name'] = storage_class_name
            storage_class['parameters']['poolname'] = os.path.join(release_ds, 'volumes')
            if await self.middleware.call('k8s.storage_class.query', [['metadata.name', '=', data['release_name']]]):
                # It should not exist already, but even if it does, that's not fatal
                await self.middleware.call(
                    'k8s.storage_class.update', f'ix-storage_class-{data["release_name"]}', storage_class
                )
            else:
                await self.middleware.call('k8s.storage_class.create', storage_class)
        except Exception:
            # Do a rollback here
            # Let's uninstall the release as well if it did get installed. TODO: do this after query
            if await self.middleware.call('pool.dataset.query', [['id', '=', release_ds]]):
                await self.middleware.call('zfs.dataset.delete', release_ds, {'recursive': True, 'force': True})
            raise

    @accepts(Str('release_name'))
    async def do_delete(self, release_name):
        # For delete we will uninstall the release first and then remove the associated datasets
        await self.middleware.call('kubernetes.validate_k8s_setup')
        await self.get_instance(release_name)

        cp = await run(['helm', 'uninstall', release_name, '-n', CHART_NAMESPACE], check=False)
        if cp.returncode:
            raise CallError(f'Unable to uninstall "{release_name}" chart release: {cp.stderr}')

        k8s_config = await self.middleware.call('kubernetes.config')
        release_ds = os.path.join(k8s_config['dataset'], 'releases', release_name)
        if await self.middleware.call('pool.dataset.query', [['id', '=', release_ds]]):
            await self.middleware.call('zfs.dataset.delete', release_ds, {'recursive': True, 'force': True})

    @private
    async def release_datasets(self, release_dataset):
        return [release_dataset] + [os.path.join(release_dataset, k) for k in ('charts', 'volumes')]
