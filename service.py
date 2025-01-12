import csv
import json
import logging
from datetime import datetime
from supabase import create_client
import schedule
import time
import chardet
import os

def setup_logging():
    """Configura o sistema de logging."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "service.log")),
            logging.StreamHandler()
        ]
    )

def detect_encoding(file_path):
    """Detecta a codificação do arquivo."""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

def load_csv_data(csv_path, required_columns):
    """Carrega os dados do arquivo CSV usando a codificação detectada."""
    try:
        encoding = detect_encoding(csv_path)
        logging.info(f"Codificação detectada: {encoding}")

        with open(csv_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=';')  # Ajuste para o delimitador correto (;)

            # Verifica se todas as colunas obrigatórias estão presentes
            if not reader.fieldnames:
                raise ValueError("Nenhuma coluna detectada no CSV.")
            
            missing_columns = [col for col in required_columns if col not in reader.fieldnames]
            if missing_columns:
                raise ValueError(f"Colunas obrigatórias ausentes no CSV: {missing_columns}")

            logging.info(f"Colunas detectadas no CSV: {reader.fieldnames}")
            data = list(reader)
            logging.info(f"Carregados {len(data)} registros do CSV.")
            return data
    except Exception as e:
        logging.error(f"Erro ao carregar dados do CSV: {e}")
        raise

def load_config():
    """Carrega o arquivo de configuração."""
    logging.info("Carregando arquivo de configuração.")
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as file:
            config = json.load(file)

        required_keys = [
            "supabase_url",
            "supabase_anon_key",
            "supabase_service_role_key",
            "csv_file_path",
            "update_interval_minutes"
        ]

        for key in required_keys:
            if key not in config or not config[key]:
                raise KeyError(f"Chave obrigatória '{key}' ausente ou vazia no arquivo de configuração.")

        return config
    except Exception as e:
        logging.error(f"Erro ao carregar config.json: {e}")
        raise

def connect_to_supabase(config):
    """Conecta ao Supabase usando as configurações fornecidas."""
    logging.info("Conectando ao Supabase.")
    try:
        client = create_client(config["supabase_url"], config["supabase_service_role_key"])
        return client
    except Exception as e:
        logging.error(f"Erro ao conectar ao Supabase: {e}")
        raise

def sync_table(supabase, table_name, unique_key, data, transform_func):
    """Sincroniza uma tabela no Supabase."""
    logging.info(f"Sincronizando tabela '{table_name}'.")
    for i, row in enumerate(data, start=1):
        try:
            record = transform_func(row)
            unique_value = record[unique_key]
            existing = supabase.table(table_name).select(unique_key).eq(unique_key, unique_value).execute()
            if not existing.data:
                supabase.table(table_name).insert(record).execute()
                logging.info(f"Registro inserido na tabela '{table_name}': {record}")
            else:
                logging.info(f"Registro já existe na tabela '{table_name}': {unique_value}")
        except KeyError as e:
            logging.error(f"Erro: Coluna ausente no registro {i}: {e}")
        except Exception as e:
            logging.error(f"Erro ao sincronizar registro {i} na tabela '{table_name}': {e}")

def transform_cliente(row):
    """Transforma uma linha de CSV em dados do cliente."""
    return {
        "id_siger_cliente": int(row["Cód"].strip()),
        "nome_cliente": row.get("Razão social", "").strip(),
        "cnpj": row.get("CNPJ/CPF", "").strip(),
        "deletado": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() != "ativo"
    }

def transform_contrato(row):
    """Transforma uma linha de CSV em dados do contrato."""
    return {
        "id_contrato": int(row["Núm.contrato"].strip()),
        "id_siger_cliente": int(row["Cód"].strip()),
        "dt_inic_cont": row.get("Dt.inc.cont", "").strip(),
        "dt_vig_inic": row.get("Dt.vig.inic", "").strip(),
        "dt_vig_final": row.get("Dt.vig.final", "2099-01-01").strip(),
        "deletado": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() != "ativo"
    }

def transform_produto(row):
    """Transforma uma linha de CSV em dados do produto."""
    return {
        "id_produto_siger": int(row["Código"].strip()),
        "nome_produto": row["Desc.item"].strip(),
        "tipo_produto": row["Descrição"].strip(),
        "num_serie": row.get("Núm.lote forn", "").strip(),
        "num_lote": row.get("Núm.lote", "").strip(),
        "ativo": row.get("Descr.Sit.item cont.(enumerado)", "").strip().lower() == "ativo"
    }

def sync_data():
    """Inicia o processo de sincronização de dados."""
    logging.info("Iniciando sincronização dos dados.")
    try:
        config = load_config()
        supabase = connect_to_supabase(config)

        required_columns = [
            "Cód", "Razão social", "CNPJ/CPF",  # Cliente
            "Núm.contrato", "Dt.inc.cont", "Dt.vig.inic", "Dt.vig.final",  # Contrato
            "Código", "Desc.item", "Descrição"  # Produto
        ]

        csv_data = load_csv_data(config["csv_file_path"], required_columns)

        sync_table(supabase, 'cliente', 'id_siger_cliente', csv_data, transform_cliente)
        sync_table(supabase, 'contrato', 'id_contrato', csv_data, transform_contrato)
        sync_table(supabase, 'produto', 'id_produto_siger', csv_data, transform_produto)

        logging.info(f"Sincronização concluída com sucesso às {datetime.now()}.")
    except Exception as e:
        logging.error(f"Erro durante a sincronização: {e}")

def start_service():
    """Inicializa o serviço de sincronização agendado."""
    config = load_config()
    interval = config["update_interval_minutes"]
    logging.info(f"Serviço iniciado. Atualização programada a cada {interval} minutos.")

    schedule.every(interval).minutes.do(sync_data)

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logging.error(f"Erro no loop de agendamento: {e}")
        time.sleep(1)

if __name__ == "__main__":
    setup_logging()
    start_service()
