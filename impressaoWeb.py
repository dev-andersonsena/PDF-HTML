import time
import cx_Oracle
import subprocess
import os
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
import datetime
import yaml

def obter_conteudo_html_from_db(usuario, senha, host, porta, sid, query, output_dir, diretorio_saida_log):
    # Construir a string de conexão Oracle
    conexao_str = f"{usuario}/{senha}@{host}:{porta}/{sid}"

    try:
        # Tentar estabelecer a conexão
        with cx_Oracle.connect(conexao_str) as connection:
            cursor = connection.cursor()

            # Executar a consulta SQL
            cursor.execute(query)

            # Iterar pelos resultados
            for resultado in cursor:
                # Obter o conteúdo HTML da linha atual e converter para string
                html_content = str(resultado[0])

                # Converter o conteúdo HTML para PDF e contar o número de páginas
                pdf_file, num_pages = converter_html_para_pdf(html_content, output_dir)

                # Salvar os resultados em log
                data_documento = resultado[1]  # Corrigido o índice para data_documento
                salvar_resultados_em_log(resultado[1], num_pages, data_documento, output_dir)

            # Fechar o cursor
            cursor.close()

    except cx_Oracle.DatabaseError as e:
        # Salvar mensagem de erro no arquivo de log
        salvar_mensagem_de_erro(f"Erro ao conectar ao banco de dados Oracle: {e}", output_dir)

def converter_html_para_pdf(html_content, output_dir, margin_top=44, font_size=12, margin_bottom=55, margin_left=16, margin_right=16):
    # Parsear o conteúdo HTML usando BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Verificar se existe uma tag <html> no HTML
    if soup.html is None:
        # Se não houver, criar uma nova tag <html> e adicioná-la ao HTML
        html_tag = soup.new_tag('html')
        soup.append(html_tag)

    # Verificar se existe uma tag <head> no HTML
    if soup.head is None:
        # Se não houver, criar uma nova tag <head> e adicioná-la ao HTML
        head_tag = soup.new_tag('head')
        soup.html.append(head_tag)

    # Criar uma tag <style> para definir o estilo do conteúdo HTML
    style_tag = soup.new_tag('style')
    style_tag.string = f'body {{ font-size: {font_size}px; }}'

    # Adicionar a tag <style> à tag <head>
    soup.head.append(style_tag)

    # Salvar o conteúdo HTML em um arquivo temporário
    html_file = os.path.join(output_dir, 'temp.html')
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(str(soup))

    # Converta o HTML para PDF usando wkhtmltopdf
    pdf_file = os.path.join(output_dir, 'pdfile.pdf')
    wkhtmltopdf_path = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf'
    cmd_line = f'"{wkhtmltopdf_path}" --page-size A4 --margin-top {margin_top}mm --margin-bottom {margin_bottom}mm --margin-left {margin_left}mm --margin-right {margin_right}mm --zoom 1.0 "{html_file}" "{pdf_file}"'
    subprocess.run(cmd_line, shell=True)

    # Verificar se o arquivo PDF foi gerado corretamente
    if os.path.exists(pdf_file):
        # Obtenha o número de páginas do PDF
        with open(pdf_file, 'rb') as f:
            reader = PdfReader(f)
            num_pages = len(reader.pages)
            return pdf_file, num_pages  # Retornar o caminho do arquivo PDF e o número de páginas
    else:
        # Salvar mensagem de erro no arquivo de log
        salvar_mensagem_de_erro("Erro ao gerar o arquivo PDF.", output_dir)
        return None, 0  # Retornar None se o PDF não for gerado com sucesso

def salvar_resultados_em_log(id_documento, num_pages, data_documento, output_dir):
    data_documento_str = data_documento.strftime("%Y%m%d")

    # Nome do arquivo CSV
    log_file_name = f"rep_webPdf_{data_documento_str}.csv"
    log_file_path = os.path.join(output_dir, log_file_name)

    # Escrever os resultados no arquivo CSV
    with open(log_file_path, "a") as log_file:
        if os.path.getsize(log_file_path) == 0:  # Verificar se o arquivo está vazio para adicionar os cabeçalhos
            log_file.write("ID; num_pag \n")

        # Escrever cada valor em colunas separadas com uma vírgula
        log_file.write(f'{id_documento}; {num_pages}\n')

def salvar_mensagem_de_erro(mensagem, output_dir):
    data_hora_atual = datetime.datetime.now().strftime("%H-%M-%S_%d-%m-%Y")

    # Nomear o arquivo de log com a data atual
    log_file_name = f"log_webPdf{data_hora_atual}.csv"
    log_file_path = os.path.join(output_dir, log_file_name)

    # Salvar a mensagem de erro no arquivo de log
    with open(log_file_path, "a") as log_file:
        log_file.write(f"Erro: {mensagem}\n")

def ler_parametros(paramfile):
    with open(paramfile, 'r') as f:
        parametros = yaml.safe_load(f)
    return parametros

def executar_script():
    # Ler os parâmetros do arquivo paramfile.yaml
    paramfile = 'paramfile.yaml'
    parametros = ler_parametros(paramfile)

    # Imprimir os parâmetros lidos do arquivo YAML
    print("Horario de Inicio:", parametros['horario_inicio'])
    print("Horario de Fim:", parametros['horario_fim'])

    # Obter o horário de início e de fim
    horario_inicio = parametros['horario_inicio']
    horario_fim = parametros['horario_fim']

    while True:
        # Obter o horário atual
        agora = datetime.datetime.now().time()

        # Verificar se está dentro do horário de execução
        if agora >= datetime.datetime.strptime(horario_inicio, '%H:%M').time() and agora <= datetime.datetime.strptime(horario_fim, '%H:%M').time():
            print("Dentro do horario de execucao. Iniciando processamento...")

            # Sua configuração de conexão Oracle
            usuario = 'anderson'
            senha = 'anderson#2023'
            host = '10.0.56.5'
            porta = '1521'
            sid = 'SEIH'

            # Diretório de saída para os arquivos de log
            output_dir = r'C:\Banco-de-dados\saidaHtml'
            diretorio_saida_log = r'C:\Banco-de-dados\saidaHtml\log'

            # Verificar se o diretório de saída existe, se não, criar
            if not os.path.exists(diretorio_saida_log):
                os.makedirs(diretorio_saida_log)

            # Definir o período para análise
            data_inicio = '20240101'
            data_fim = '20240220'

            # Sua query para obter conteúdo HTML do banco de dados com base no período
            query = f"SELECT doc.conteudo AS pagina, pro.dta_geracao AS DATA FROM sei.documento_conteudo doc JOIN sei.documento do ON doc.id_documento = do.id_documento JOIN sei.protocolo pro ON pro.id_protocolo = do.id_documento WHERE doc.conteudo IS NOT NULL AND pro.dta_geracao BETWEEN TO_DATE('{data_inicio}', 'YYYYMMDD') AND TO_DATE('{data_fim}', 'YYYYMMDD')"

            # Obtém o conteúdo HTML do banco de dados e exibe cada linha analisada
            obter_conteudo_html_from_db(usuario, senha, host, porta, sid, query, output_dir, diretorio_saida_log)

            # Salvar o início e o fim do processamento no log
            salvar_inicio_e_fim_do_processamento(data_inicio, diretorio_saida_log)
            break  # Saia do loop após a execução

        else:
            print("Fora do horario de execucao. Aguardando...")
            # Aguardar um intervalo antes de verificar novamente
            time.sleep(10)  # Aguarda 1 minuto antes de verificar novamente

def salvar_inicio_e_fim_do_processamento(data_inicio, diretorio_saida_log):
    data_hora_atual = datetime.datetime.now().strftime("%H:%M:%S %Y%m%d")

    # Nomear o arquivo de log com a data atual
    log_file_name = 'log_webPdf.log'
    log_file_path = os.path.join(diretorio_saida_log, log_file_name)

    # Salvar o início e o fim do processamento no arquivo de log
    with open(log_file_path, "a") as log_file:
        log_file.write(f"{data_hora_atual} Inicio do processamento para o dia {data_inicio}\n")
        log_file.write(f"{data_hora_atual} Fim do processamento para o dia {data_inicio}\n")

# Executar o script
executar_script()
