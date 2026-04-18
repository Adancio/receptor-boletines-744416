import boto3
import uuid
import ast
import time
from botocore.exceptions import BotoCoreError, ClientError

expediente = "744416"

sqs = boto3.client("sqs", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
sns = boto3.client("sns", region_name="us-east-1")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/910039462201/cola-boletines"
TABLE_NAME = "boletines"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:910039462201:notificaciones-boletines"
MOSTRADOR_URL = "http://100.48.1.199:8002"


def procesar_mensaje(msg):
    try:
        body = ast.literal_eval(msg["Body"])
    except Exception as e:
        print(f"Error al parsear mensaje: {str(e)}")
        return

    boletin_id = str(uuid.uuid4())

    try:
        tabla = dynamodb.Table(TABLE_NAME)
        tabla.put_item(Item={
            "boletin_id": boletin_id,
            "contenido": body["contenido"],
            "correoElectronico": body["correoElectronico"],
            "imagen_url": body["imagen_url"],
            "leido": False,
        })
        print(f"Boletín {boletin_id} guardado en DynamoDB")
    except (BotoCoreError, ClientError) as e:
        print(f"Error al guardar en DynamoDB: {str(e)}")
        return

    try:
        link = f"{MOSTRADOR_URL}/boletines/{boletin_id}?correoElectronico={body['correoElectronico']}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Nuevo boletín disponible",
            Message=f"Se ha generado un nuevo boletín. Puedes verlo aquí:\n{link}",
        )
        print(f"Correo enviado a {body['correoElectronico']}")
    except (BotoCoreError, ClientError) as e:
        print(f"Error al enviar SNS: {str(e)}")


def consumir():
    print("Receptor escuchando cola SQS...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )
            mensajes = response.get("Messages", [])
            for msg in mensajes:
                procesar_mensaje(msg)
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"],
                )
        except (BotoCoreError, ClientError) as e:
            print(f"Error al leer de SQS: {str(e)}")
        time.sleep(2)


if __name__ == "__main__":
    print("Servicio receptor listo")
    consumir()