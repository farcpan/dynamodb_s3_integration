import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { FirehoseClient, PutRecordCommand } from "@aws-sdk/client-firehose";

export const handler = async (event: any, context: any) => {
  // const bucketName = process.env["BUCKET_NAME"];
  const streamName = process.env["STREAM_NAME"];
  if (!streamName) {
    console.error("No stream_name.");
    return;
  }
  const now = new Date().toISOString();

  const records = event.Records;

  const csvData: { id: string; dataType: string }[] = [];
  for (const record of records) {
    const eventId = record.eventId;
    const eventName = record.eventName;
    const dynamodb = record.dynamodb;

    //console.log(eventName);
    //console.log(dynamodb);

    if (eventName === "REMOVE") {
      const id = dynamodb.OldImage.id.S;
      const dataType = dynamodb.OldImage.dataType.S;
      csvData.push({ id: id, dataType: dataType });
    }
  }

  if (csvData.length > 0) {
    const csvLine =
      "Id,DataType\n" +
      csvData
        .map((csvDataInfo) => {
          return `${csvDataInfo.id},${csvDataInfo.dataType}`;
        })
        .join("\n");

    /*
    const client = new S3Client();
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: "data/" + now + ".csv",
      Body: csvLine,
    });
    await client.send(command);
    */
    const client = new FirehoseClient();
    const command = new PutRecordCommand({
      DeliveryStreamName: streamName,
      Record: {
        Data: Buffer.from(csvLine),
      },
    });
    await client.send(command);
  }
};
