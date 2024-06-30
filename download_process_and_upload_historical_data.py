import os  # Biblioteca estándar para operaciones del sistema de archivos
import requests  # Biblioteca para realizar solicitudes HTTP y descargar archivos
import zipfile  # Biblioteca para manejar archivos ZIP
import json  # Biblioteca para trabajar con datos JSON
import apache_beam as beam  # Biblioteca principal para definir y ejecutar pipelines de Apache Beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions  # Opciones de configuración de Apache Beam y Google Cloud
from google.cloud import storage, bigquery  # Bibliotecas para interactuar con Google Cloud Storage y BigQuery
from google.oauth2 import service_account  # Biblioteca para manejar autenticación con cuentas de servicio de Google Cloud

# Configuración de la autenticación
# Ruta al archivo JSON con las credenciales de la cuenta de servicio
key_path = "D:\\Automatizacion_BigData\\qwiklabs-gcp-00-d2425aaaa127-ee7693a29e45.json"

# Establece una variable de entorno para que las bibliotecas de Google Cloud utilicen estas credenciales automáticamente
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Inicialización del cliente de Storage y BigQuery
# Carga las credenciales desde el archivo JSON
credentials = service_account.Credentials.from_service_account_file(key_path)

# Inicializa el cliente para interactuar con Google Cloud Storage
storage_client = storage.Client(credentials=credentials)

# Inicializa el cliente para interactuar con BigQuery
bigquery_client = bigquery.Client(credentials=credentials)

# Crear un bucket en Google Cloud Storage
def create_bucket(bucket_name):
    # Obtiene una referencia al bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Si el bucket no existe, se crea
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} creado.")
    else:
        # Si el bucket ya existe, se notifica
        print(f"Bucket {bucket_name} ya existe.")
    
    # Devuelve la referencia al bucket
    return bucket

# Crear un dataset en BigQuery
def create_dataset(dataset_name):
    # Obtiene una referencia al dataset
    dataset_ref = bigquery_client.dataset(dataset_name)
    try:
        # Intenta obtener el dataset si ya existe
        dataset = bigquery_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_name} ya existe.")
    except Exception as e:
        # Si el dataset no existe, se crea
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print(f"Dataset {dataset_name} creado.")
    
    # Devuelve la referencia al dataset
    return dataset

# Función para descargar archivos ZIP desde una URL
def download_zip_files(url, download_to):
    """
    Descarga archivos ZIP desde una URL de API y los guarda en un directorio especificado.
    """
    # Realiza una solicitud GET a la URL
    response = requests.get(url)
    if response.status_code == 200:  # Verifica si la solicitud fue exitosa
        # Convierte la respuesta JSON a un diccionario de Python
        data = response.json()
        for resource in data['result']['resources']:  # Itera sobre los recursos en la respuesta JSON
            # Obtiene la URL del archivo ZIP
            file_url = resource['url']
            # Construye el nombre del archivo
            file_name = os.path.join(download_to, file_url.split('/')[-1])
            # Realiza una solicitud GET para descargar el archivo
            file_response = requests.get(file_url)
            if file_response.status_code == 200:  # Verifica si la descarga fue exitosa
                # Abre un archivo en modo escritura binaria
                with open(file_name, 'wb') as f:
                    # Escribe el contenido descargado en el archivo
                    f.write(file_response.content)
                print(f"Descargado {file_name}")  # Imprime un mensaje de confirmación
            else:
                # Imprime un mensaje de error si la descarga falla
                print(f"Error al descargar {file_url}")
    else:
        # Lanza una excepción si la solicitud inicial falla
        raise Exception(f"Failed to access API endpoint {url}")

# Función para extraer archivos ZIP en subdirectorios separados
def extract_zip_files(zip_dir, extract_to):
    """
    Extrae todos los archivos ZIP de un directorio especificado en subdirectorios separados.
    """
    for zip_file in os.listdir(zip_dir):  # Itera sobre cada archivo en el directorio ZIP
        # Construye la ruta completa del archivo ZIP
        zip_path = os.path.join(zip_dir, zip_file)
        # Crea un subdirectorio para cada ZIP
        sub_dir = os.path.join(extract_to, os.path.splitext(zip_file)[0])
        # Crea el subdirectorio si no existe
        os.makedirs(sub_dir, exist_ok=True)
        # Abre el archivo ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extrae todo el contenido del archivo ZIP al subdirectorio
            zip_ref.extractall(sub_dir)
        print(f"Extraído {zip_path} en {sub_dir}")  # Imprime un mensaje de confirmación

# Función para consolidar archivos TXT
def consolidate_txt_files(src_dir, dest_dir):
    """
    Consolida archivos TXT de múltiples carpetas en archivos únicos por nombre de archivo.
    """
    if not os.path.exists(dest_dir):  # Verifica si el directorio de destino no existe
        # Crea el directorio de destino
        os.makedirs(dest_dir)
    
    # Obtener una lista de archivos únicos en src_dir
    txt_files = set()
    for root, _, files in os.walk(src_dir):  # Recorre el directorio de origen y sus subdirectorios
        for file in files:
            if file.endswith('.txt'):  # Agrega archivos TXT únicos al conjunto
                txt_files.add(file)
    
    for txt_file in txt_files:  # Itera sobre cada nombre de archivo TXT
        # Abre el archivo de destino en modo escritura
        with open(os.path.join(dest_dir, txt_file), 'w', encoding='utf-8') as dest_file:
            first_file = True
            for root, _, files in os.walk(src_dir):  # Recorre el directorio de origen y sus subdirectorios
                if txt_file in files:  # Si el archivo TXT está en la lista de archivos
                    # Abre el archivo fuente en modo lectura
                    with open(os.path.join(root, txt_file), 'r', encoding='utf-8') as src_file:
                        # Lee todas las líneas del archivo fuente
                        lines = src_file.readlines()
                        if first_file:  # Si es el primer archivo
                            # Escribe todas las líneas (incluyendo el encabezado)
                            dest_file.writelines(lines)
                            first_file = False
                        else:
                            # Omitir el encabezado en archivos posteriores
                            dest_file.writelines(lines[1:])
        print(f"Archivo consolidado: {txt_file}")  # Imprime un mensaje de confirmación

# Función para subir archivos a Google Cloud Storage
def upload_to_gcs(source_file, bucket_name):
    """
    Sube un archivo a un bucket de Google Cloud Storage.
    """
    # Crea un cliente de Google Cloud Storage
    client = storage.Client()
    # Obtiene el bucket especificado
    bucket = client.bucket(bucket_name)
    # Crea un blob en el bucket con el nombre del archivo fuente
    blob = bucket.blob(os.path.basename(source_file))
    # Sube el archivo desde la ruta local al blob en el bucket
    blob.upload_from_filename(source_file)
    print(f"Archivo {source_file} subido a GCS")  # Imprime un mensaje de confirmación

def main():
    # Configuración
    # URL del endpoint de la API
    endpoint = 'https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et'
    # Directorio donde se guardarán los archivos ZIP descargados
    zip_dir = 'zips_api'
    # Directorio donde se extraerán los archivos ZIP
    extract_to = 'zips_api_extracted'
    # Directorio donde se guardarán los archivos TXT consolidados
    consolidated_dir = 'txt_consolidated'
    # Definir nombres del bucket y del dataset
    bucket_name = "bucket_transporte_9"
    dataset_id = "dataset_transporte_9"

    # Crear el bucket y el dataset
    create_bucket(bucket_name)
    create_dataset(dataset_id)

    # Crear directorios si no existen
    os.makedirs(zip_dir, exist_ok=True)
    os.makedirs(extract_to, exist_ok=True)
    os.makedirs(consolidated_dir, exist_ok=True)

    # Descargar archivos ZIP desde la API
    download_zip_files(endpoint, zip_dir)

    # Extraer los archivos ZIP descargados en subdirectorios separados
    extract_zip_files(zip_dir, extract_to)

    # Consolidar archivos TXT
    consolidate_txt_files(extract_to, consolidated_dir)

    # Subir archivos consolidados a Google Cloud Storage (Bucket)
    for txt_file in os.listdir(consolidated_dir):
        upload_to_gcs(os.path.join(consolidated_dir, txt_file), bucket_name)

    print("Proceso completado.")

if __name__ == "__main__":
    main()
