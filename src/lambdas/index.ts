import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { FirehoseClient, PutRecordCommand } from "@aws-sdk/client-firehose";

export const handler = async (event: any, context: any) => {
  // const bucketName = process.env["BUCKET_NAME"];
  const streamName = process.env["STREAM_NAME"];
  if (!streamName) {
    console.error("No stream_name.");
    return;
  }
  const records = event.Records;

  const jsonData: {
    id: string;
    dataType: string;
    eventId: string;
  }[] = [];
  for (const record of records) {
    const eventId = record.eventId;
    const eventName = record.eventName;
    const dynamodb = record.dynamodb;

    //console.log(eventName);
    //console.log(dynamodb);

    if (eventName === "REMOVE") {
      const id = dynamodb.OldImage.id.S;
      const dataType = dynamodb.OldImage.dataType.S;
      jsonData.push({
        id: id,
        dataType: dataType,
        eventId: eventId,
      });
    }
  }

  const client = new FirehoseClient();
  for (const data of jsonData) {
    const command = new PutRecordCommand({
      DeliveryStreamName: streamName,
      Record: {
        Data: Buffer.from(JSON.stringify(data)),
      },
    });
    await client.send(command);
  }
};
