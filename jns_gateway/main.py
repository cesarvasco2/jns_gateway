import sys
import json
import paho.mqtt.client as mqtt
import boto3
import time
from datetime import date, datetime
import calendar
#from operations import Operations
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='processador_entrada')
#configurações do broker:
Broker = 'servermqtt.duckdns.org'
PortaBroker = 1883 
Usuario = 'afira'
Senha = 'afira'
KeepAliveBroker = 60
TopicoSubscribe = 'JNSOLUCOES/#' #Topico que ira se inscrever
#Callback - conexao ao broker realizada
def on_connect(client, userdata, flags, rc):
    print('[STATUS] Conectado ao Broker. Resultado de conexao: {}'.format(str(rc)))
#faz subscribe automatico no topico
    client.subscribe(TopicoSubscribe)
#Callback - mensagem recebida do broker
def on_message(client, userdata, msg):
    MensagemRecebida = str(msg.payload.decode('utf-8') )
    #print("[MESAGEM RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+MensagemRecebida)
    dict_payload = json.loads(MensagemRecebida)
    lista_de_campos = [
        {'key':'temperatura','type':'int','fields':'temperatura'},
        {'key':'nivel','type':'int','fields':'nivel'},
        {'key':'status','type':'int','fields':'status'},
        {'key':'vazao','type':'array','fields':'vazao'},
        {'key':'pressao','type':'array','fields':('pressao',)},
        {'key':'valvulas','type':'int','fields':'valvulas'},
        {'key':'horimetro','type':'array','fields':('horimetro',)},
        {'key':'defeito','type':'int','fields':'defeito'},
    ]
    dia_semana = date.today() 
    data_e_hora_atuais = datetime.now() 
    dict_save = {}
    dict_save['id_dispositivo'] = msg.topic.split('/')[1]
    dict_save['data_hora_dispositivo'] = data_e_hora_atuais.strftime('%Y-%m-%d %H:%M:%S')
    for campo in lista_de_campos:
        if isinstance(campo['fields'],tuple):
            elementos_do_campo = []
            if campo['type']=='array':
                for field in campo['fields']:
                    if field in dict_payload:
                        elementos_do_campo.append(dict_payload[field])
            dict_save[campo['key']] = elementos_do_campo                
        else:
            if campo['type']=='str':
                if campo['fields'] in dict_payload:
                    dict_save[campo['key']] = str(dict_payload[campo['fields']])
            else:
                if campo['type']=='int':
                    if campo['fields'] in dict_payload:
                        dict_save[campo['key']] = dict_payload[campo['fields']]
    dict_save['codigo_produto'] = 22
    dict_save['timestamp_dispositivo'] = int(datetime.now().timestamp())
    dict_save['timestamp_servidor'] = int(datetime.now().timestamp())
    dict_save['dia_sem'] = calendar.day_name[dia_semana.weekday()]
    queue.send_message(MessageBody=str(json.dumps(dict_save, ensure_ascii=False)))
    print(dict_save)
try:
    print('[STATUS] Inicializando MQTT...')
#inicializa MQTT:
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(Usuario, Senha)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_forever()

except KeyboardInterrupt:
    print ('\nCtrl+C pressionado, encerrando aplicacao e saindo...')
    sys.exit(0)