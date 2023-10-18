import argparse
import base64
import os
import pathlib

from ascend.sdk import definitions, field, value
from ascend.sdk.applier import DataflowApplier
from ascend.sdk.client import Client

import ascend.protos.ascend.ascend_pb2 as ascend
import ascend.protos.component.component_pb2 as component
import ascend.protos.connection.connection_pb2 as connection
import ascend.protos.content_encoding.content_encoding_pb2 as content_encoding
import ascend.protos.core.core_pb2 as core
import ascend.protos.environment.environment_pb2 as environment
import ascend.protos.expression.expression_pb2 as expression
import ascend.protos.format.format_pb2 as format
import ascend.protos.function.function_pb2 as function
import ascend.protos.io.io_pb2 as io
import ascend.protos.operator.operator_pb2 as operator
import ascend.protos.pattern.pattern_pb2 as pattern
import ascend.protos.schema.schema_pb2 as schema
import ascend.protos.text.text_pb2 as text

from google.protobuf.wrappers_pb2 import DoubleValue
from google.protobuf.wrappers_pb2 import BoolValue
from google.protobuf.wrappers_pb2 import Int64Value
from google.protobuf.wrappers_pb2 import UInt64Value
from google.protobuf.wrappers_pb2 import Int32Value
from google.protobuf.wrappers_pb2 import UInt32Value
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import NullValue
from google.protobuf.empty_pb2 import Empty


GENERATED_FROM_HOSTNAME = "doit-playground.ascend.io"
GENERATED_FROM_DATA_SERVICE_ID = "Matt_Test"
GENERATED_FROM_DATAFLOW_ID = "UNIQUE_WEATHER_BY_DAY_TAKE_3"


def construct_dataflow(data_service_id: str = GENERATED_FROM_DATA_SERVICE_ID, dataflow_id: str = GENERATED_FROM_DATAFLOW_ID):
  ## Data feed connectors for this dataflow
  data_feed_connectors = []


  ## Data share connectors for this dataflow
  data_share_connectors = []


  ## Components for this dataflow
  components = []

  component_weather_ALL_FILES_copy_copy = definitions.ReadConnector(
    id='weather_ALL_FILES_copy_copy',
    name='weather ALL FILES copy copy',
    description='',
    pattern=pattern.Pattern(
      glob='**',
    ),
    container=io.Container(
      record_connection=io.Connection.Asset.Record(
        connection_id=connection.Id(
          value='Matt_GCP_Weather',
        ),
        details={
          'bucket': value.String('ascend-bakeoff'),
          'object_pattern': value.String('weather_2023'),
          'object_pattern_type': value.Union(prefix={}),
          'parser_type': value.Union(csv={
            'header': value.Bool(True),
          }),
        },
      ),
    ),
    update_periodical=core.Periodical(
      period=Duration(
        seconds=134217727,
      ),
      offset=Duration(
        seconds=81497099,
      ),
    ),
    last_manual_refresh_time=Timestamp(
      seconds=1696455117,
    ),
    assigned_priority=component.Priority(
    ),
    aggregation_limit=None,
    bytes=None,
    records=component.Source.FromRecords(
      schema=schema.Map(
        field=[
          field.String('id'),
          field.Double('latitude'),
          field.Double('longitude'),
          field.Double('elevation'),
          field.String('state'),
          field.String('name'),
          field.String('gsn_flag'),
          field.String('hcn_crn_flag'),
          field.String('wmoid'),
          field.String('source_url'),
          field.String('etl_timestamp'),
          field.String('date'),
          field.String('element'),
          field.Int('value'),
          field.String('mflag'),
          field.String('qflag'),
          field.String('sflag'),
          field.Int('time'),
        ],
      ),
    ),
    compute_configurations=[
      expression.StageComputeConfiguration(
        stage=expression.Stage(
          read=expression.Stage.Read(
          ),
        ),
        configuration=operator.ComputeConfiguration(
          tests=function.QualityTests(
            standard=[
              function.QualityTests.StandardCheck(
                column='id',
                rule='NOT_NULL',
                value=ascend.Value(
                  bool_value=True,
                ),
              ),
              function.QualityTests.StandardCheck(
                column='id',
                rule='UNIQUE',
                value=ascend.Value(
                  bool_value=True,
                ),
              ),
              function.QualityTests.StandardCheck(
                column='state',
                rule='NOT_NULL',
                value=ascend.Value(
                  bool_value=True,
                ),
              ),
            ],
            input=1,
          ),
        ),
      ),
    ],
    non_materialized=False,
  )
  components.append(component_weather_ALL_FILES_copy_copy)

  component_weather_ALL_FILES_copy_transform_copy = definitions.Transform(
    id='weather_ALL_FILES_copy_transform_copy',
    name='weather ALL FILES copy Transform copy',
    description='Transform from weather ALL FILES copy',
    input_ids=[
      'weather_ALL_FILES_copy_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES_copy_transform_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES_copy_transform_copy)

  component_weather_ALL_FILES___LUKEWARM_copy = definitions.Transform(
    id='weather_ALL_FILES___LUKEWARM_copy',
    name='weather ALL FILES - LUKEWARM copy',
    description='Transform from weather ALL FILES copy Transform',
    input_ids=[
      'weather_ALL_FILES_copy_transform_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES___LUKEWARM_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES___LUKEWARM_copy)

  component_weather_ALL_FILES_copy___COLD_copy = definitions.Transform(
    id='weather_ALL_FILES_copy___COLD_copy',
    name='weather ALL FILES copy - COLD copy',
    description='Transform from weather ALL FILES copy Transform',
    input_ids=[
      'weather_ALL_FILES_copy_transform_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES_copy___COLD_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES_copy___COLD_copy)

  component_weather_ALL_FILES_copy_transform_transform_copy = definitions.Transform(
    id='weather_ALL_FILES_copy_transform_transform_copy',
    name='weather ALL FILES copy Transform Transform copy',
    description='Transform from weather ALL FILES copy Transform',
    input_ids=[
      'weather_ALL_FILES_copy_transform_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES_copy_transform_transform_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES_copy_transform_transform_copy)

  component_weather_ALL_FILES___LUKEWARM_transform_copy = definitions.Transform(
    id='weather_ALL_FILES___LUKEWARM_transform_copy',
    name='weather ALL FILES - LUKEWARM Transform copy',
    description='Transform from weather ALL FILES - LUKEWARM',
    input_ids=[
      'weather_ALL_FILES___LUKEWARM_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES___LUKEWARM_transform_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES___LUKEWARM_transform_copy)

  component_weather_ALL_FILES_copy___COLD_transform_copy = definitions.Transform(
    id='weather_ALL_FILES_copy___COLD_transform_copy',
    name='weather ALL FILES copy - COLD Transform copy',
    description='Transform from weather ALL FILES copy - COLD',
    input_ids=[
      'weather_ALL_FILES_copy___COLD_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES_copy___COLD_transform_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES_copy___COLD_transform_copy)

  component_weather_ALL_FILES_copy_transform_transform_transform_copy = definitions.Transform(
    id='weather_ALL_FILES_copy_transform_transform_transform_copy',
    name='weather ALL FILES copy Transform Transform Transform copy',
    description='Transform from weather ALL FILES copy Transform Transform',
    input_ids=[
      'weather_ALL_FILES_copy_transform_transform_copy',
    ],
    operator=operator.Operator(
      spark_function=operator.Spark.Function(
        executable=io.Executable(
          code=io.Code(
            language=function.Code.Language(
              bigquery_sql=function.Code.Language.BigQuerySql(
              ),
            ),
            source=io.Code.Source(
              inline=pathlib.Path(os.path.join(os.path.dirname(os.path.realpath(__file__)), "components", "weather_ALL_FILES_copy_transform_transform_transform_copy.sql")).read_bytes().decode("utf-8"),
            ),
          ),
        ),
        reduction=operator.Reduction(
          full=operator.Reduction.Full(
          ),
        ),
        tests=function.QualityTests(
        ),
      ),
    ),
    assigned_priority=component.Priority(
    ),
  )
  components.append(component_weather_ALL_FILES_copy_transform_transform_transform_copy)

  component__Matt_BQ_Weather____Lukewarm_Output = definitions.WriteConnector(
    id='_Matt_BQ_Weather____Lukewarm_Output',
    name='[Matt BQ Weather] - Lukewarm Output',
    description='',
    input_id='weather_ALL_FILES___LUKEWARM_transform_copy',
    container=io.Container(
      record_connection=io.Connection.Asset.Record(
        connection_id=connection.Id(
          value='Matt_BQ_Weather',
        ),
        details={
          'dataset': value.String('weather_data'),
          'output_table': value.String('LUKEWARM_WEATHER'),
          'schema_mismatch': value.Union(display_error={}),
        },
      ),
    ),
    assigned_priority=component.Priority(
    ),
    bytes=None,
    records=component.Sink.ToRecords(
    ),
    compute_configurations=None,
  )
  components.append(component__Matt_BQ_Weather____Lukewarm_Output)

  component__Matt_BQ_Weather____Cold_Output = definitions.WriteConnector(
    id='_Matt_BQ_Weather____Cold_Output',
    name='[Matt BQ Weather] - Cold Output',
    description='',
    input_id='weather_ALL_FILES_copy___COLD_transform_copy',
    container=io.Container(
      record_connection=io.Connection.Asset.Record(
        connection_id=connection.Id(
          value='Matt_BQ_Weather',
        ),
        details={
          'dataset': value.String('weather_data'),
          'output_table': value.String('COLD_WEATHER'),
          'schema_mismatch': value.Union(display_error={}),
        },
      ),
    ),
    assigned_priority=component.Priority(
    ),
    bytes=None,
    records=component.Sink.ToRecords(
    ),
    compute_configurations=None,
  )
  components.append(component__Matt_BQ_Weather____Cold_Output)

  component_HOT_WEATHER_Output___BigQuery = definitions.WriteConnector(
    id='HOT_WEATHER_Output___BigQuery',
    name='HOT WEATHER Output - BigQuery',
    description='',
    input_id='weather_ALL_FILES_copy_transform_transform_transform_copy',
    container=io.Container(
      record_connection=io.Connection.Asset.Record(
        connection_id=connection.Id(
          value='Matt_BQ_Weather',
        ),
        details={
          'dataset': value.String('weather_data'),
          'output_table': value.String('HOT_WEATHER'),
          'schema_mismatch': value.Union(display_error={}),
        },
      ),
    ),
    assigned_priority=component.Priority(
    ),
    bytes=None,
    records=component.Sink.ToRecords(
    ),
    compute_configurations=None,
  )
  components.append(component_HOT_WEATHER_Output___BigQuery)


  ## Data feeds for this dataflow
  data_feeds = []


  ## Data shares for this dataflow
  data_shares = []


  ## Component groups for this dataflow
  groups = []


  return definitions.Dataflow(
    id=dataflow_id,
    name="UNIQUE_WEATHER_BY_DAY_TAKE_3" if dataflow_id == GENERATED_FROM_DATAFLOW_ID else dataflow_id,
    description='',
    components=components,
    data_feeds=data_feeds,
    data_feed_connectors=data_feed_connectors,
    data_shares=data_shares,
    data_share_connectors=data_share_connectors,
    groups=groups,
  )


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--hostname", default=os.getenv("ASCEND_HOSTNAME", default=GENERATED_FROM_HOSTNAME))
  parser.add_argument("-ds", "--data-service-id", default=os.getenv("ASCEND_DATA_SERVICE_ID", default=GENERATED_FROM_DATA_SERVICE_ID))
  parser.add_argument("-df", "--dataflow-id", default=os.getenv("ASCEND_DATAFLOW_ID", default=GENERATED_FROM_DATAFLOW_ID))
  parser.add_argument("--no-delete", action='store_true')
  parser.add_argument("--dry-run", action='store_true')
  args = parser.parse_args()

  client = Client(args.hostname)
  dataflow = construct_dataflow(args.data_service_id, args.dataflow_id)
  DataflowApplier(client).apply(args.data_service_id, dataflow, delete=(not args.no_delete), dry_run=args.dry_run)