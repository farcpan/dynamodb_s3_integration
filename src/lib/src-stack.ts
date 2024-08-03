import { Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  AttributeType,
  BillingMode,
  StreamViewType,
  Table,
} from "aws-cdk-lib/aws-dynamodb";
import { join } from "path";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { Runtime, StartingPosition } from "aws-cdk-lib/aws-lambda";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import {
  Effect,
  ManagedPolicy,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { CfnCrawler, CfnTable } from "aws-cdk-lib/aws-glue";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { Database, S3Table, Schema, DataFormat } from "@aws-cdk/aws-glue-alpha";
import { CfnWorkGroup } from "aws-cdk-lib/aws-athena";

// 参考:
// https://dev.classmethod.jp/articles/dynamic-partitioning-of-output-data-using-dynamic-partitioning-on-amazon-kinesis-data-firehose-aws-cdk/

const systemName = "dynamodb-s3-integration";

interface SrcStackProps {}

export class SrcStack extends Stack {
  constructor(scope: Construct, id: string, props: SrcStackProps) {
    super(scope, id, props);

    // S3
    const bucketId = systemName + "-bucket";
    const bucket = new Bucket(this, bucketId, {
      bucketName: bucketId,
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // DynamoDB
    const tableId = systemName + "-db";
    const table = new Table(this, tableId, {
      tableName: tableId,
      billingMode: BillingMode.PAY_PER_REQUEST,
      partitionKey: {
        name: "id",
        type: AttributeType.STRING,
      },
      sortKey: {
        name: "dataType",
        type: AttributeType.STRING,
      },
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
      stream: StreamViewType.NEW_AND_OLD_IMAGES,
      timeToLiveAttribute: "timestamp",
    });

    // Firehose
    const deliveryStreamId = systemName + "-firehose";
    const deliveryStream = new CfnDeliveryStream(this, deliveryStreamId, {
      deliveryStreamName: deliveryStreamId,
      deliveryStreamType: "DirectPut",
      extendedS3DestinationConfiguration: {
        bucketArn: bucket.bucketArn,
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 128,
        },
        dynamicPartitioningConfiguration: {
          enabled: true,
        },
        prefix: "data/!{partitionKeyFromQuery:id}/",
        errorOutputPrefix: "error/",
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: "MetadataExtraction",
              parameters: [
                {
                  parameterName: "MetadataExtractionQuery", //クエリ文字列
                  parameterValue: "{id: .id}",
                },
                {
                  parameterName: "JsonParsingEngine", //putされたデータをjqエンジンでクエリする
                  parameterValue: "JQ-1.6",
                },
              ],
            },
            {
              type: "AppendDelimiterToRecord",
              parameters: [
                {
                  parameterName: "Delimiter",
                  parameterValue: "\\n",
                },
              ],
            },
          ],
        },
        compressionFormat: "UNCOMPRESSED",
        roleArn: new Role(this, systemName + "-iam-role-for-stream", {
          assumedBy: new ServicePrincipal("firehose.amazonaws.com"),
          managedPolicies: [
            ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
          ],
        }).roleArn,
      },
    });

    // Lambda
    const path = join(__dirname, "../lambdas/index.ts");
    const dynamodbTriggeredLambdaId = systemName + "-lambda-func";
    const dynamodbTriggeredLambdaFunction = new NodejsFunction(
      this,
      dynamodbTriggeredLambdaId,
      {
        functionName: dynamodbTriggeredLambdaId,
        runtime: Runtime.NODEJS_20_X,
        timeout: Duration.seconds(30),
        logRetention: RetentionDays.ONE_DAY,
        entry: path,
        handler: "handler",
        environment: {
          BUCKET_NAME: bucket.bucketName,
          STREAM_NAME: deliveryStream.deliveryStreamName ?? "",
        },
      }
    );

    // DynamoDB Stream Trigger
    table.grantStreamRead(dynamodbTriggeredLambdaFunction); // Stream Read
    dynamodbTriggeredLambdaFunction.addEventSource(
      new DynamoEventSource(table, {
        startingPosition: StartingPosition.LATEST,
      })
    );

    // Lambda -> S3
    // bucket.grantPut(dynamodbTriggeredLambdaFunction); // Lambda -> S3 put object

    // Lambda -> Firehose
    dynamodbTriggeredLambdaFunction.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["firehose:PutRecord"],
        resources: [deliveryStream.attrArn],
      })
    );

    // カタログ
    const dataCatalog = new Database(this, systemName + "-data-catalog", {
      databaseName: systemName + "-data-catalog-db",
    });
    // データカタログテーブル
    const dataGlueTable = new S3Table(this, systemName + "-s3-table", {
      tableName: systemName + "-s3-table",
      database: dataCatalog,
      bucket: bucket,
      s3Prefix: "data/",
      partitionKeys: [
        {
          name: "id",
          type: Schema.STRING,
        },
      ],
      dataFormat: DataFormat.JSON,
      columns: [
        {
          name: "dataType",
          type: Schema.STRING,
        },
      ],
    });

    // Athenaワークグループ
    new CfnWorkGroup(this, systemName + "athena-workgroup", {
      name: systemName + "athena-workgroup",
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${bucket.bucketName}/results`,
        },
      },
      recursiveDeleteOption: true,
    });

    /*
      AWS Glue Crawler
    */
    /*
    const crawlerRole = new Role(this, systemName + "-crawler-role", {
      assumedBy: new ServicePrincipal("glue.amazonaws.com"),
    });
    crawlerRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole")
    );
    crawlerRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        resources: [`${bucket.bucketArn}/data/*`],
        actions: ["s3:GetObject", "s3:PutObject"],
      })
    );

    const crawlerName = systemName + "-crawler";
    const crawler = new CfnCrawler(this, crawlerName, {
      role: crawlerRole.roleArn,
      databaseName: systemName + "-athena-db",
      targets: {
        s3Targets: [{ path: `${bucket.s3UrlForObject()}/data/` }],
      },
      configuration:
        '{"Version": 1.0, "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"}}',
    });
    */
  }
}
