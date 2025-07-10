# web_scraping_api.py
import requests
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    try:
        current_year = datetime.now().year
        url = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{current_year}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            return {
                'statusCode': 500,
                'body': 'Formato de datos inesperado del API del IGP'
            }

        sismos = sorted(data, key=lambda x: (x['fecha_local'], x['hora_local']), reverse=True)[:10]
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('t_sismos')

        scan = table.scan()
        with table.batch_writer() as batch:
            for item in scan['Items']:
                batch.delete_item(Key={'id': item['id']})

        for sismo in sismos:
            item = {
                'id': str(uuid.uuid4()),
                'codigo': sismo.get('codigo', 'N/A'),
                'fecha_local': sismo.get('fecha_local', 'N/A'),
                'hora_local': sismo.get('hora_local', 'N/A'),
                'magnitud': str(sismo.get('magnitud', 0.0)),
                'profundidad': str(sismo.get('profundidad', 'N/A')),
                'referencia': sismo.get('referencia', 'N/A'),
                'reporte_pdf': sismo.get('reporte_acelerometrico_pdf', ''),
                'timestamp': datetime.now().isoformat()
            }
            table.put_item(Item=item)

        return {
            'statusCode': 200,
            'body': {
                'message': f"{len(sismos)} sismos almacenados en DynamoDB",
                'sismos': sismos
            }
        }

    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': f"Error al conectarse al API del IGP: {str(e)}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error inesperado: {str(e)}"
        }
