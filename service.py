import csv
import json
import logging
from datetime import datetime
from supabase import create_client, Client
import schedule
import time
import chardet

logging.info(f"Colunas detectadas no CSV: {list(reader.fieldnames)}")


def detect_encoding(file_path):
    """Detecta a codificação do arquivo."""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

def load_csv_data(csv_path):
    """Carrega dados do CSV com a codificação correta."""
    try:
        # Detecta a codificação do arquivo
        encoding = detect_encoding(csv_path)
        logging.info(f"Codificação detectada: {encoding}")

        # Lê o arquivo CSV com a codificação detectada
        with open(csv_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file)
            data = list(reader)
            logging.info(f"Carregados {len(data)} registros do CSV.")
            return data
    except Exception as e:
        logging.error(f"Erro ao carregar dados do CSV: {e}")
        raise

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("service.log"),
        logging.StreamHandler()
    ]
)

# Função para carregar configurações
def load_config():
    logging.info("Carregando arquivo de configuração.")
    try:
        with open('config.json', 'r') as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Erro ao carregar config.json: {e}")
        raise

# Função para conectar ao Supabase
def connect_to_supabase(config):
    logging.info("Conectando ao Supabase.")
    try:
        client = create_client(config["supabase_url"], config["supabase_service_role_key"])
        return client
    except Exception as e:
        logging.error(f"Erro ao conectar ao Supabase: {e}")
        raise

# Função para carregar dados do CSV
def load_csv_data(csv_path):
    logging.info(f"Lendo dados do arquivo CSV: {csv_path}")
    try:
        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = list(reader)
            logging.info(f"Carregados {len(data)} registros do CSV.")
            return data
    except Exception as e:
        logging.error(f"Erro ao carregar dados do CSV: {e}")
        raise

# Função para verificar e inserir clientes
def sync_clientes(supabase, data):
    """Sincroniza a tabela cliente no Supabase."""
    logging.info("Sincronizando clientes.")
    for row in data:
        # Mapear a coluna 'cód' do CSV para 'id_siger_cliente'
        cliente_data = {
            "id_siger_cliente": int(row["cód"]),  # Mapeando 'cód' para id_siger_cliente
            "nome_cliente": row["nome_cliente"],
            "cnpj": row["cnpj"],
            "deletado": row.get("deletado", "false").lower() == "true"
        }
        try:
            # Verificar se o cliente já existe
            existing = supabase.table('cliente').select("id_siger_cliente").eq("id_siger_cliente", cliente_data["id_siger_cliente"]).execute()
            if not existing.data:
                # Inserir novo cliente
                supabase.table('cliente').insert(cliente_data).execute()
                logging.info(f"Cliente inserido: {cliente_data}")
            else:
                logging.info(f"Cliente já existe: {cliente_data['id_siger_cliente']}")
        except Exception as e:
            logging.error(f"Erro ao sincronizar cliente {cliente_data['id_siger_cliente']}: {e}")

def load_csv_data(csv_path):
    """Carrega os dados do arquivo CSV usando a codificação detectada."""
    try:
        # Detecta a codificação do arquivo
        encoding = detect_encoding(csv_path)

        # Lê o arquivo CSV com a codificação detectada
        with open(csv_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file)
            logging.info(f"Colunas detectadas no CSV: {list(reader.fieldnames)}")  # Log para verificar cabeçalhos
            data = list(reader)
            logging.info(f"Carregados {len(data)} registros do CSV.")
            return data
    except Exception as e:
        logging.error(f"Erro ao carregar dados do CSV: {e}")
        raise

# Função para verificar e inserir contratos
def sync_contratos(supabase, data):
    logging.info("Sincronizando contratos.")
    for row in data:
        contrato_data = {
            "id_contrato": int(row["id_contrato"]),
            "id_siger_cliente": int(row["id_siger_cliente"]),
            "dt_inic_cont": row["dt_inic_cont"],
            "dt_vig_inic": row["dt_vig_inic"],
            "dt_vig_final": row.get("dt_vig_final", "2099-01-01"),
            "deletado": row.get("deletado", "false").lower() == "true"
        }
        try:
            existing = supabase.table('contrato').select("id_contrato").eq("id_contrato", contrato_data["id_contrato"]).execute()
            if not existing.data:
                supabase.table('contrato').insert(contrato_data).execute()
                logging.info(f"Contrato inserido: {contrato_data}")
            else:
                logging.info(f"Contrato já existe: {contrato_data['id_contrato']}")
        except Exception as e:
            logging.error(f"Erro ao sincronizar contrato {contrato_data['id_contrato']}: {e}")

# Função para verificar e inserir produtos
def sync_produtos(supabase, data):
    logging.info("Sincronizando produtos.")
    for row in data:
        produto_data = {
            "id_produto_siger": int(row["id_produto_siger"]),
            "nome_produto": row["nome_produto"],
            "tipo_produto": row["tipo_produto"],
            "id_cliente_siger": int(row["id_cliente_siger"]),
            "id_contrato": int(row["id_contrato"]),
            "num_serie": row.get("num_serie"),
            "num_lote": row.get("num_lote"),
            "deletado": row.get("deletado", "false").lower() == "true",
            "ativo": row.get("ativo", "true").lower() == "true"
        }
        try:
            existing = supabase.table('produto').select("id_produto_siger").eq("id_produto_siger", produto_data["id_produto_siger"]).execute()
            if not existing.data:
                supabase.table('produto').insert(produto_data).execute()
                logging.info(f"Produto inserido: {produto_data}")
            else:
                logging.info(f"Produto já existe: {produto_data['id_produto_siger']}")
        except Exception as e:
            logging.error(f"Erro ao sincronizar produto {produto_data['id_produto_siger']}: {e}")

# Função principal de sincronização
def sync_data():
    logging.info("Iniciando sincronização dos dados.")
    try:
        config = load_config()
        supabase = connect_to_supabase(config)
        csv_data = load_csv_data(config["csv_file_path"])
        
        sync_clientes(supabase, csv_data)
        sync_contratos(supabase, csv_data)
        sync_produtos(supabase, csv_data)

        logging.info(f"Sincronização concluída com sucesso às {datetime.now()}")
    except Exception as e:
        logging.error(f"Erro durante a sincronização: {e}")

# Agendar execução com base no intervalo configurado
def start_service():
    config = load_config()
    interval = config["update_interval_minutes"]
    logging.info(f"Serviço iniciado. Atualização programada a cada {interval} minutos.")
    
    schedule.every(interval).minutes.do(sync_data)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    start_service()
