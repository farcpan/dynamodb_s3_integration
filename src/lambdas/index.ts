import { FirehoseClient, PutRecordCommand } from "@aws-sdk/client-firehose";

export const handler = async (event: any, context: any) => {
  const streamName = process.env["STREAM_NAME"];
  if (!streamName) {
    console.error("No stream_name.");
    return;
  }
  const records = event.Records;

  const csvData: string[] = [];
  for (const record of records) {
    // const eventId = record.eventId;
    const eventName = record.eventName;
    const dynamodb = record.dynamodb;

    if (eventName === "INSERT") {
      const id = dynamodb.NewImage.id.S;
      const dataType = dynamodb.NewImage.dataType.S;
      const timestamp = dynamodb.NewImage.timestamp.N;
      const ttl = dynamodb.NewImage.ttl.N;
      csvData.push(`${id},${dataType},${timestamp},${ttl}\n`);
    }
  }

  const client = new FirehoseClient();
  for (const data of csvData) {
    const command = new PutRecordCommand({
      DeliveryStreamName: streamName,
      Record: {
        Data: Buffer.from(data),
      },
    });
    await client.send(command);
  }
};
