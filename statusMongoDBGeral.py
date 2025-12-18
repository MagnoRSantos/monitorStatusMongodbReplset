#import sys
import os, io 
import json
from pymongo import MongoClient
from datetime import datetime, timedelta
import dotenv
import socket
import logging
from mssql_python import connect

## IMPORTS PROPRIOS
from sendMsgChatGoogle import sendMsgChatGoogle
from removeLogAntigo import removeLogs

## variaveis globais 
formatData = '%Y-%m-%d %H:%M:%S'
dirapp = os.path.dirname(os.path.realpath(__file__))

## Carrega os valores do .env
dotenvFile = os.path.join(dirapp, '.env.prod')
dotenv.load_dotenv(dotenvFile)


## Path LogFile
datahoraLog = datetime.now().strftime('%Y-%m-%d')
pathLog = os.path.join(dirapp, 'log')
pathLogFile = os.path.join(pathLog, 'logServerStatusMongoDB_{}.txt'.format(datahoraLog))

if not os.path.exists(pathLog):
    os.makedirs(pathLog)

logging.basicConfig(
	filemode='a',
	filename=pathLogFile,
	format="[%(asctime)s] [%(levelname)s] - %(message)s",
	datefmt="%m-%d-%Y %H:%M:%S",
	level=logging.INFO,
	encoding='utf-8'
)

#logger = logging.getLogger(__name__)


## funcao de remocao de arquivos de logs antigos
def removerLogAntigo(v_diasRemover):
    """
    FUNCAO DE REMOCAO DE ARQUIVOS DE LOG ANTIGOS
    ACIMA DE X DIAS: v_diasRemover
    """
    ## remocao dos logs antigos acima de xx dias
    pathLog = os.path.join(dirapp, 'log')
    msgLog = "Removendo logs acima de {0} dias.".format(v_diasRemover)
    logging.info(msgLog)
    msgLog = removeLogs(v_diasRemover, pathLog)
    logging.info(msgLog)

## funcao de envio de alerta de exception ao google chat via webhook
def enviaExceptionGChat(msgGChat):
    """
    FUNCAO DE ENVIO DE DADOS REFERENTE E EXCEPTION
    ESSES DADOS SAO ENVIADOS AO GOOGLE CHAT DA EQUIPE RESPONSAVEL
    CRIADO PARA QUANDO DER FALHA A EQUIPE SER AVISADA
    """
    URL_WEBHOOK_ALERT = getValueEnv("URL_WEBHOOK_ALERT")
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    myhost = socket.gethostname()

    msgWebHook = 'Host: {0} - EXCEPTION - MONITORAMENTO DE DADOS SERVER STATUS/REPLICASET STATUS MONGODB - {1}\nMensagem: {2}'.format(myhost, datahora, msgGChat)
    sendMsgChatGoogle(URL_WEBHOOK_ALERT, msgWebHook)

## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():
    """
        OBTEM DATA E HORA
    """
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora


## funcao de mensagem inicial da aplicacao
def msgInitialApp():
	"""
	FUNCAO QUE IMPRIME E GRAVA EM LOG MSG INICIAL DA APLICACAO
	"""
	datahora = obterDataHora()
	msgLog = 'BEGIN - List Info ServerStatus/ReplicaSet Status MongoDB'
	print("[{0}] - {1}".format(datahora, msgLog))
	logging.info(msgLog)


## funcao de mensagem final da aplicacao
def msgFinalApp():
	"""
	FUNCAO QUE IMPRIME E GRAVA EM LOG MSG FINAL DA APLICACAO
	"""
	datahora = obterDataHora()
	msgLog = 'END - List Info ServerStatus/ReplicaSet Status MongoDB'
	print("[{0}] - {1}".format(datahora, msgLog))
	logging.info(msgLog)


## funcao para verificar os valores do dotenv
def getValueEnv(valueEnv):
	"""
	OBTEM VALORES DO ARQUIVO .ENV
	"""
	v_valueEnv = os.getenv(valueEnv)
	
	if not v_valueEnv: 
		msgLog = "Variável de ambiente '{0}' não encontrada.".format(valueEnv)
		logging.error(msgLog)
	
	return v_valueEnv

## calcula diferenca entre datas para obter atraso do replicaset
def calculaAtrasoReplSet(v_dataInicial, v_optimeDate):
	diffDatas = (datetime.strptime(v_dataInicial, formatData) - datetime.strptime(v_optimeDate, formatData)).total_seconds()
	return int(diffDatas)

## converte lista para JSON
def listToJson(v_namereplset, listServers):
	"""
	FUNCAO CRIADA PARA TRANSFORMA OS DADOS OBTIDOS EM FORMATO DE LISTA PARA JSON
	"""
	# Definição das chaves do JSON - 19 itens (0 a 18)
	chaves = [
			"version", #0
			"storageEngine", #1
			"member", #2
			"host", #3
			"name", #4
			"stateStr", #5
			"syncSourceHost", #6
			"uptime", #7
			"optimeDate", #8
			"optimeDate(secs)", #9
			"activeSessionsCount", #10
			"collections", #11
			"indexStatsCount", #12
			"views", #13
			"defaultReadConcernLevel", #14
			"defaultWriteConcernW", #15
			"defaultWriteConcernWTimeout", #16
			"flowControlEnabled", #17
			"flowControlTargetRateLimit" #18
		]
	
	#membros_dict = [dict(zip(chaves, item)) for item in listServers]

	membros_dict = []
	for item in listServers:
		membros_dict.append(dict(zip(chaves, item)))

	dados_json_dict = {
		"replicaset_name": v_namereplset,
    	"replicaset_members": membros_dict
	}

	json_output = json.dumps(dados_json_dict, indent=4, default=str)

	return json_output


## obtem dados do comando serverStatus
def getInfoServerStatus(v_nameserver, v_namereplset):

	v_uptime = None
	v_activeSessionsCount = None
	
	try:

		## Dados de conexao mongodb
		user = getValueEnv('USERNAME_MONGODB')
		pwd = getValueEnv('PASSWORD_MONGODB')
		db_authdb = getValueEnv('DBAUTHDB_MONGODB')

		# obtem o uptime do servidor
		connstr = "mongodb://{0}:{1}@{2}/{3}?replicaSet={4}&directConnection=true".format(user, pwd, v_nameserver, db_authdb, v_namereplset)
		with MongoClient(connstr) as client:
			db_server = client ['admin']
			v_serverStatus = db_server.command("serverStatus")
			
			## obtem uptime
			v_uptime = v_serverStatus["uptime"]
			v_uptime = timedelta(seconds=int(v_uptime))

			## obtem activeSessionsCount
			v_activeSessionsCount = v_serverStatus["logicalSessionRecordCache"]["activeSessionsCount"]

			# nome do host
			v_host = v_serverStatus["host"]
			
			# versao mongodb
			v_version = v_serverStatus["version"]

			# quantidade de colecoes
			v_collections = v_serverStatus["catalogStats"]["collections"]

			# quantidade de views
			v_views = v_serverStatus["catalogStats"]["views"]
			
			# ReadConcern configurado
			v_defaultReadConcernLevel = v_serverStatus["defaultRWConcern"]["defaultReadConcern"]["level"]
			
			# WriteConcern configurado
			v_defaultWriteConcernW = v_serverStatus["defaultRWConcern"]["defaultWriteConcern"]["w"]
			v_defaultWriteConcernWTIMEOUT = v_serverStatus["defaultRWConcern"]["defaultWriteConcern"]["wtimeout"]
			
			# Informacoes flowControl
			v_flowControl = v_serverStatus["flowControl"]["enabled"]
			v_flowControltargetRateLimit = v_serverStatus["flowControl"]["targetRateLimit"]
			
			# Informacoes de indices - Quantidade
			v_indexStats = v_serverStatus["indexStats"]["count"]

			# Informacoes storageEngine - wiredTiger
			v_storageEngine = v_serverStatus["storageEngine"]["name"]

			dictServerStatus = {
				"v_uptime": v_uptime, 
				"v_activeSessionsCount": v_activeSessionsCount, 
				"v_host": v_host, 
				"v_version": v_version, 
				"v_collections": v_collections, 
				"v_indexStats": v_indexStats,
				"v_views": v_views, 
				"v_defaultReadConcernLevel": v_defaultReadConcernLevel, 
				"v_defaultWriteConcernW": v_defaultWriteConcernW, 
				"v_defaultWriteConcernWTIMEOUT": v_defaultWriteConcernWTIMEOUT, 
				"v_flowControl": v_flowControl, 
				"v_flowControltargetRateLimit": v_flowControltargetRateLimit, 
				"v_storageEngine": v_storageEngine
			}
	
	except Exception as e:
		msgLog = "Erro ao obter dados do MongoDB: {0}".format(e)
		logging.error(msgLog)
		enviaExceptionGChat(msgLog)
		dictServerStatus = None
	
	finally:
		if client:
			client.close()
		
		return dictServerStatus


## obtem dados do comando replSetStatus
def getInfoReplSetStatus():

	listFinal = []
	v_namereplset = None

	try:
		## Dados de coinexao mongodb
		user = getValueEnv('USERNAME_MONGODB')
		pwd = getValueEnv('PASSWORD_MONGODB')
		db_authdb = getValueEnv('DBAUTHDB_MONGODB')
		servidores = getValueEnv('SERVER_MONGODB')
		
		connstr = "mongodb://{0}:{1}@{2}/{3}".format(user, pwd, servidores, db_authdb)
		with MongoClient(connstr) as client:
			db = client ['admin']
			rs_stats = db.command({'replSetGetStatus': 1})
			v_namereplset = str(rs_stats["set"])
			
			## antigo for members
			#nserver = (0,1,2)
			#for n in nserver:

			## novo for members dinamico
			lista_membros = rs_stats.get("members", [])
			for n in range(len(lista_membros)):
				
				# lista auxiliar para insercao dos dados
				listAux = []
				
				# obtem dados do status do replicaset
				v_member = str(n)
				v_name = str(rs_stats["members"][n]["name"])
				v_stateStr = str(rs_stats["members"][n]["stateStr"])
				v_syncSourceHost = 'IsPrimary' if str(rs_stats["members"][n]["syncSourceHost"]) == "" else str(rs_stats["members"][n]["syncSourceHost"])
				
				## obtem tempo de optimeDate do primario
				if v_syncSourceHost == 'IsPrimary':
					dtInicialPrimary = str(rs_stats["members"][n]["optimeDate"])
					v_optimeDate = dtInicialPrimary
					v_optimeDate_secs = 0
					msgLog = "optimeDate primary: {0}".format(dtInicialPrimary)
					logging.info(msgLog)

				## obtem tempo de optimeDate do replicaset nos secundarios e realiza o calculo do atraso	
				if v_syncSourceHost != 'IsPrimary':
					dtInicial = dtInicialPrimary
					v_optimeDate = str(rs_stats["members"][n]["optimeDate"])
					msgLog = "optimeDate primary: {0} || optimeDate secondary: {1}".format(dtInicial, v_optimeDate)
					logging.info(msgLog)

					## calcula atraso do replicaset
					v_optimeDate_secs = calculaAtrasoReplSet(dtInicial, v_optimeDate)	

				## obtem dados do serverStatus
				dictServerStatus = getInfoServerStatus(v_name, v_namereplset)

				# obtem dados do dicionario para variaveis
				v_uptime = dictServerStatus.get("v_uptime")
				v_activeSessionsCount = dictServerStatus.get("v_activeSessionsCount")
				v_host = dictServerStatus.get("v_host")
				v_version = dictServerStatus.get("v_version")
				v_collections = dictServerStatus.get("v_collections")
				v_indexStats = dictServerStatus.get("v_indexStats")
				v_views = dictServerStatus.get("v_views")
				v_defaultReadConcernLevel = dictServerStatus.get("v_defaultReadConcernLevel")
				v_defaultWriteConcernW = dictServerStatus.get("v_defaultWriteConcernW")
				v_defaultWriteConcernWTIMEOUT = dictServerStatus.get("v_defaultWriteConcernWTIMEOUT")
				v_flowControl = dictServerStatus.get("v_flowControl")
				v_flowControltargetRateLimit = dictServerStatus.get("v_flowControltargetRateLimit")
				v_storageEngine = dictServerStatus.get("v_storageEngine")
				
				# insere dados na lista auxiliar
				listAux.insert(0, v_version)
				listAux.insert(1, v_storageEngine)
				listAux.insert(2, v_member)
				listAux.insert(3, v_host)
				listAux.insert(4, v_name)
				listAux.insert(5, v_stateStr)
				listAux.insert(6, v_syncSourceHost)
				listAux.insert(7, str(v_uptime))
				listAux.insert(8, v_optimeDate)
				listAux.insert(9, v_optimeDate_secs)
				listAux.insert(10, v_activeSessionsCount)
				listAux.insert(11, v_collections)
				listAux.insert(12, v_indexStats)
				listAux.insert(13, v_views)
				listAux.insert(14, v_defaultReadConcernLevel)
				listAux.insert(15, v_defaultWriteConcernW)
				listAux.insert(16, v_defaultWriteConcernWTIMEOUT)
				listAux.insert(17, v_flowControl)
				listAux.insert(18, v_flowControltargetRateLimit)
				
				# insere dados na lista final
				listFinal.append(listAux)

	except Exception as e:
		msgLog = "Erro ao obter dados do MongoDB: {0}".format(e)
		logging.error(msgLog)
		enviaExceptionGChat(msgLog)

	finally:
		## fecha conexoes se necessario
		if client:
			client.close()

		return v_namereplset, listFinal


## funcao de formacao da connString Database de destino
def strConnectionDatabaseDestino():
    
    #variaveis de conexao azuresql
    v_server   = getValueEnv("SERVER_TARGET_SQL")
    v_port     = getValueEnv("PORT_TARGET_SQL")
    v_database = getValueEnv("DATABASE_TARGET_SQL")
    v_username = getValueEnv("USERNAME_TARGET_SQL")
    v_password = getValueEnv("PASSWORD_TARGET_SQL")

    strConnection = "Server=tcp:{server},{port};Database={database};Uid={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"\
        .format(server = v_server, port = v_port, database = v_database, username = v_username, password = v_password)

    return strConnection


## FUNCAO DE INSERT DE DADOS NO DATABASE DE DESTINO
def gravaDadosDestinoAzureSQL(v_namereplset, v_listReturnMongoDB):

	connString = str(strConnectionDatabaseDestino())
	
	try:
    	
		"""
		-- Modelo de Criação da tabela de destino no SQL
		CREATE TABLE [dbo].[monitorStatusMongoDBGeral] (
			replicasetName VARCHAR(20),
			version VARCHAR(10),
			storageEngine VARCHAR(10),
			member SMALLINT,
			host VARCHAR(15),
			name VARCHAR(30),
			stateStr VARCHAR(20),
			syncSourceHost VARCHAR(30),
			uptime VARCHAR(60),
			optimeDate DATETIME,
			optimeDate_secs BIGINT,
			activeSessionsCount BIGINT,
			collections INT,
			indexStatsCount INT,
			views INT,
			defaultReadConcernLevel VARCHAR(10),
			defaultWriteConcernW VARCHAR(10),
			defaultWriteConcernWTimeout INT,
			flowControlEnabled BIT,
			flowControlTargetRateLimit BIGINT,
			dataColeta DATETIME
		);

		-- GRANT SELECT, UPDATE, DELETE, INSERT ON [dbo].[monitorStatusMongoDBGeral] TO [usr.monitoramento];
		"""

		## OPEN CONNECTION
		with connect(connString, autocommit=False) as cnxn:

            ## CREATE A CURSOR FROM THE CONNECTION
			with cnxn.cursor() as cursor:

                ## sql statement DELETE
				sqlcmdDELETE = "DELETE FROM [dbo].[monitorStatusMongoDBGeral];"
				cursor.execute(sqlcmdDELETE)

                ## sql statement INSERT
				sql_insert  = '''
					INSERT INTO [dbo].[monitorStatusMongoDBGeral]
					(   
						replicasetName,
						version,
						storageEngine,
						member,
						host,
						name,
						stateStr,
						syncSourceHost,
						uptime,
						optimeDate,
						optimeDate_secs,
						activeSessionsCount,
						collections,
						indexStatsCount,
						views,
						defaultReadConcernLevel,
						defaultWriteConcernW,
						defaultWriteConcernWTimeout,
						flowControlEnabled,
						flowControlTargetRateLimit,
						dataColeta 
					) 
					VALUES 
					(
						'{0}', ?, ?, ?, ?, ?, 
						?, ?, ?, ?, ?, 
						?, ?, ?, ?, ?,
						?, ?, ?, ?, '{1}'
					);
				'''.format(v_namereplset, obterDataHora())

				RowCount = 0
				for params in v_listReturnMongoDB:
					cursor.execute(sql_insert, params)
					RowCount = RowCount + cursor.rowcount

			## Commit the transaction 
			cnxn.commit()
        
	except Exception as e:

		## Rollback em caso de erro
		try:
			cnxn.rollback()
		except Exception:
			pass

		datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		msgException = "Error: {0}".format(e)
		msgLog = 'Fim inserção de dados no destino[SQL] - [Erro]: {0}\n{1}'.format(datahora, msgException)
		logging.error(msgLog)
		enviaExceptionGChat(msgLog)
        
	finally:
		## Close the database connection
		if cnxn:
			cnxn.close()
			msgLog = "Conexão com SQL encerrada."
			logging.info(msgLog)
        
		if cursor:
			cursor.close()
			del cursor
			msgLog = "Cursor encerrado."
			logging.info(msgLog)
		
		datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		msgLog = 'Quantidade de Registros Inseridos [SQL]: {0} registro(s)\n'.format(RowCount)
		msgLog = '{0}Fim inserção de dados no destino [SQL] - {1}'.format(msgLog, datahora)
		logging.info(msgLog)


## função principal
def main():

	# mensagem inicial da aplicacao
	msgInitialApp()

	## remover logs antigos acima de xx dias
	diasRemover = 10
	removerLogAntigo(diasRemover)

	try:
		msgLog = "Iniciando processo de coleta de dados do replicaset..."
		logging.info(msgLog)
		v_namereplset, rs_stats = getInfoReplSetStatus()
		gravaDadosDestinoAzureSQL(v_namereplset, rs_stats)
	
	except Exception as e:
		msgLog = "Erro ao obter dados do MongoDB: {0}".format(e)
		logging.error(msgLog)
		enviaExceptionGChat(msgLog)
	
	else:
		msgLog = "\n{0}".format(listToJson(v_namereplset, rs_stats))
		logging.info(msgLog)

	finally:
		# mensagem final da aplicacao
		msgFinalApp()

#inicio da aplicacao
if __name__ == "__main__":
	main()


