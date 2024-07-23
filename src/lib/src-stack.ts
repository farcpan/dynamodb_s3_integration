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
import { CfnCrawler } from "aws-cdk-lib/aws-glue";

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
    bucket.grantPut(dynamodbTriggeredLambdaFunction); // Lambda -> S3 put object

    /*
      AWS Glue Crawler
    */
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
  }
}
