#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import logging
import requests

from zmon_worker_monitor.zmon_worker.errors import CheckError
from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


logging.getLogger('botocore').setLevel(logging.WARN)


class DataPipelineWrapperFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(DataPipelineWrapperFactory, self).__init__()

    def configure(self, conf):
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(DataPipelineWrapper, region=factory_ctx.get('entity').get('region', None))


def get_region():
    r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=3)
    return r.json()['region']


# create a dict of keys from a list of dicts
def create_dict_from_list_of_fields(fields):
    fields_dict = {}
    for field in fields:
        fields_dict[str(field['key']).replace('@', '')] = str(field['stringValue'])

    return fields_dict


class DataPipelineWrapper(object):
    def __init__(self, region=None):
        if not region:
            region = get_region()
        self.__client = boto3.client('datapipeline', region_name=region)

    def list_pipelines(self, pipeline_names=None):
        """
        List all the pipelines with their name and id.
        :param pipeline_names: Pipeline names as a String or a list of Strings
        :type pipeline_names: str or list

        :return: All the pipelines for this AWS account.
        :rtype: s/map/dict
        """

        if pipeline_names is None:
            pipeline_names = []

        if isinstance(pipeline_names, str) or pipeline_names is None:
            pipeline_names = [pipeline_names]
        elif not isinstance(pipeline_names, list):
                raise CheckError('Parameter "pipeline_names" should be a string or a list of strings '
                                 'denoting pipeline names')

        pipelines = self.__client.list_pipelines()
        return {pipeline['name']: pipeline['id']
                for pipeline in pipelines['pipelineIdList']
                if not pipeline_names or pipeline['name'] in pipeline_names}

    def describe_pipelines(self, pipeline_ids=None, pipeline_names=None):
        """
        Return a list of pipelines with their details.
        :param pipeline_ids: Pipeline IDs as a String or list of Strings.
        :type pipeline_ids: str or list

        :param pipeline_names: Pipeline names as a String or list of Strings
        :type pipeline_names: str or list

        :return Details from the requested pipelines
        :rtype: s/map/dict
        """

        if pipeline_ids is None:
            pipeline_ids = []

        if isinstance(pipeline_ids, str):
            pipeline_ids = [pipeline_ids]
        elif not isinstance(pipeline_ids, list):
                raise CheckError('Parameter "pipeline_ids" should be a string or a list of strings '
                                 'denoting pipeline IDs')

        if pipeline_names is None:
            pipeline_names = []

        if isinstance(pipeline_names, str):
            pipeline_names = [pipeline_names]
        elif not isinstance(pipeline_names, list):
                raise CheckError('Parameter "pipeline_names" should be a string or a list of strings '
                                 'denoting pipeline names')

        if not pipeline_ids and not pipeline_names:
            pipeline_ids = self.list_pipelines().values()
        elif pipeline_names:
            # add all the IDs retrieved from their names
            pipeline_ids.extend(self.list_pipelines(pipeline_names).values())

        # make sure we don't have duplicate IDs *sorted is for testing purposes*
        pipeline_ids = sorted(list(set(pipeline_ids)))

        response = self.__client.describe_pipelines(pipelineIds=pipeline_ids)

        # parse the response and manipulate data to return the pipeline id and its description fields
        pipelines_states = [(str(pipeline['pipelineId']), create_dict_from_list_of_fields(pipeline['fields']))
                            for pipeline in response['pipelineDescriptionList']]
        result = {}

        if not pipelines_states:
            return result

        # create a dict of pipeline_id : details_map
        for (pipeline_id, pipeline_details) in pipelines_states:
            result[pipeline_id] = pipeline_details

        # returns a map which has the pipeline IDs as keys and their details as values
        return result

    def describe_all_pipelines(self):
        """
        Return a list of pipelines with their details.
        :return: Details from all the pipelines
        :rtype: s/map/dict
        """
        return self.describe_pipelines(pipeline_ids=self.list_pipelines().values())
