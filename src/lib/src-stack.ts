import { Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  AttributeType,
  BillingMode,
  StreamViewType,
  Table,
} from "aws-cdk-lib/aws-dynamodb";
import { join } from "path";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { Runtime, StartingPosition } from "aws-cdk-lib/aws-lambda";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

const systemName = "dynamodb-s3-integration";

interface SrcStackProps {}

export class SrcStack extends Stack {
  constructor(scope: Construct, id: string, props: SrcStackProps) {
    super(scope, id, props);

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
      }
    );

    // DynamoDB Stream Trigger
    table.grantStreamRead(dynamodbTriggeredLambdaFunction); // Stream Read
    dynamodbTriggeredLambdaFunction.addEventSource(
      new DynamoEventSource(table, {
        startingPosition: StartingPosition.LATEST,
      })
    );
  }
}
