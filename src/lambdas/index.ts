export const handler = async (event: any, context: any) => {
  const records = event.Records;
  for (const record of records) {
    console.log(record);
  }
};
