const AWS = require("aws-sdk");
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const apiGateway = new AWS.ApiGatewayManagementApi({
  endpoint: process.env.WEBSOCKET_ENDPOINT,
});

exports.handler = async (event) => {
  for (const record of event.Records) {
    if (record.eventName === "INSERT") {
      const newItem = AWS.DynamoDB.Converter.unmarshall(
        record.dynamodb.NewImage
      );

      if (newItem.type === "email") {
        const message = {
          action: "newEmail",
          data: newItem,
        };

        const connectionIds = await getConnectionIdsFromDynamoDB();
        const sendPromises = connectionIds.map(async (connectionId) => {
          try {
            await apiGateway
              .postToConnection({
                ConnectionId: connectionId,
                Data: JSON.stringify(message),
              })
              .promise();
          } catch (error) {
            if (error.statusCode === 410) {
              await removeConnectionIdFromDynamoDB(connectionId);
            } else {
              console.error("Error sending message to client:", error);
            }
          }
        });

        await Promise.all(sendPromises);
      }
    }
  }
};

async function getConnectionIdsFromDynamoDB() {
  const params = {
    TableName: "WebSocketConnections",
    ProjectionExpression: "connectionId",
  };

  const result = await dynamoDb.scan(params).promise();
  return result.Items.map((item) => item.connectionId);
}

async function removeConnectionIdFromDynamoDB(connectionId) {
  const params = {
    TableName: "WebSocketConnections",
    Key: { connectionId },
  };

  await dynamoDb.delete(params).promise();
}
