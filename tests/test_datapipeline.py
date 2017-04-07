from zmon_worker_monitor.builtins.plugins.datapipeline import DataPipelineWrapper

from mock import MagicMock

list_pipelines_result = {
  'pipelineIdList': [
    {u'id': u'df-00627471SOVYZEXAMPLE', 'name': 'my-pipeline'},
    {u'id': u'df-00627471SOFDLKASJDDF', 'name': 'my-pipeline2'},
    {u'id': u'df-0870198233ZYVEXAMPLE', 'name': 'CrossRegionDDB'},
    {u'id': u'df-00189603TB4MZEXAMPLE', 'name': 'CopyRedshift'}
  ]
}

describe_pipelines_result_single = {
  'pipelineDescriptionList': [
    {
      'fields': [
        {u'stringValue': 'PENDING', 'key': '@pipelineState'},
        {u'stringValue': 'my-pipeline', 'key': 'name'},
        {'stringValue': 'my-pipeline-token', 'key': 'uniqueId'}
      ],
      'pipelineId': 'df-00627471SOVYZEXAMPLE',
      'name': 'my-pipeline',
      'tags': []
    }
  ]
}

describe_pipelines_result_multiple = {
  'pipelineDescriptionList': [
    {
      'fields': [
        {u'stringValue': 'PENDING', 'key': '@pipelineState'},
        {u'stringValue': 'my-pipeline', 'key': 'name'},
        {'stringValue': 'my-pipeline-token', 'key': 'uniqueId'}
      ],
      'pipelineId': 'df-00627471SOVYZEXAMPLE',
      'name': 'my-pipeline',
      'tags': []
    },
    {
      'fields': [
        {u'stringValue': 'SCHEDULED', 'key': '@pipelineState'},
        {u'stringValue': 'my-pipeline2', 'key': 'name'},
        {'stringValue': 'my-pipeline-token2', 'key': 'uniqueId'}
      ],
      'pipelineId': 'df-00627471SOFDLKASJDDF',
      'name': 'my-pipeline2',
      'tags': []
    }
  ]
}


def test_list_pipelines_no_parameter(monkeypatch):
    client = MagicMock()
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.list_pipelines()
    assert elb_data == {'my-pipeline': 'df-00627471SOVYZEXAMPLE',
                        'my-pipeline2': 'df-00627471SOFDLKASJDDF',
                        'CrossRegionDDB': 'df-0870198233ZYVEXAMPLE',
                        'CopyRedshift': 'df-00189603TB4MZEXAMPLE'}

    client.list_pipelines.assert_called_with()


def test_list_pipelines_one_pipeline(monkeypatch):
    client = MagicMock()
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.list_pipelines(pipeline_names='my-pipeline')
    assert elb_data == {'my-pipeline': 'df-00627471SOVYZEXAMPLE'}

    client.list_pipelines.assert_called_with()


def test_list_pipelines_multiple_pipelines(monkeypatch):
    client = MagicMock()
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.list_pipelines(pipeline_names=['my-pipeline', 'my-pipeline2'])
    assert elb_data == {'my-pipeline': 'df-00627471SOVYZEXAMPLE', 'my-pipeline2': 'df-00627471SOFDLKASJDDF'}

    client.list_pipelines.assert_called_with()


def test_describe_pipelines_one_pipeline(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_single

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_pipelines(pipeline_ids='df-00627471SOVYZEXAMPLE')
    assert elb_data == {'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.describe_pipelines.assert_called_with(pipelineIds=['df-00627471SOVYZEXAMPLE'])


def test_describe_pipelines_multiple_pipelines(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_multiple

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_pipelines(pipeline_ids=['df-00627471SOVYZEXAMPLE', 'df-00627471SOFDLKASJDDF'])
    assert elb_data == {'df-00627471SOFDLKASJDDF': {'name': 'my-pipeline2',
                                                    'uniqueId': 'my-pipeline-token2',
                                                    'pipelineState': 'SCHEDULED'},
                        'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.describe_pipelines.assert_called_with(pipelineIds=['df-00627471SOFDLKASJDDF', 'df-00627471SOVYZEXAMPLE'])


def test_describe_pipelines_multiple_pipelines_with_names_ids(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_multiple
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_pipelines(pipeline_ids=['df-00627471SOVYZEXAMPLE'], pipeline_names='my-pipeline2')
    assert elb_data == {'df-00627471SOFDLKASJDDF': {'name': 'my-pipeline2',
                                                    'uniqueId': 'my-pipeline-token2',
                                                    'pipelineState': 'SCHEDULED'},
                        'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.list_pipelines.assert_called_with()
    client.describe_pipelines.assert_called_with(pipelineIds=['df-00627471SOFDLKASJDDF', 'df-00627471SOVYZEXAMPLE'])


def test_describe_pipelines_single_pipeline_names(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_single
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_pipelines(pipeline_names='my-pipeline')
    assert elb_data == {'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.list_pipelines.assert_called_with()
    client.describe_pipelines.assert_called_with(pipelineIds=['df-00627471SOVYZEXAMPLE'])


def test_describe_pipelines_multiple_pipelines_duplicate_names_ids(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_single
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_pipelines(pipeline_ids=['df-00627471SOVYZEXAMPLE'], pipeline_names=['my-pipeline'])
    assert elb_data == {'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.list_pipelines.assert_called_with()
    client.describe_pipelines.assert_called_with(pipelineIds=['df-00627471SOVYZEXAMPLE'])


def test_describe_all_pipelines(monkeypatch):
    client = MagicMock()
    client.describe_pipelines.return_value = describe_pipelines_result_multiple
    client.list_pipelines.return_value = list_pipelines_result

    get = MagicMock()
    get.return_value.json.return_value = {'region': 'myregion'}
    monkeypatch.setattr('requests.get', get)
    monkeypatch.setattr('boto3.client', lambda x, region_name: client)
    datapipeline = DataPipelineWrapper()
    elb_data = datapipeline.describe_all_pipelines()

    # there should be more items here but it's enough for the purpose of the test
    assert elb_data == {'df-00627471SOFDLKASJDDF': {'name': 'my-pipeline2',
                                                    'uniqueId': 'my-pipeline-token2',
                                                    'pipelineState': 'SCHEDULED'},
                        'df-00627471SOVYZEXAMPLE': {'name': 'my-pipeline',
                                                    'uniqueId': 'my-pipeline-token',
                                                    'pipelineState': 'PENDING'}}

    client.list_pipelines.assert_called_with()
    client.describe_pipelines.assert_called_with(pipelineIds=['df-00189603TB4MZEXAMPLE', 'df-00627471SOFDLKASJDDF',
                                                              'df-00627471SOVYZEXAMPLE', 'df-0870198233ZYVEXAMPLE'])
