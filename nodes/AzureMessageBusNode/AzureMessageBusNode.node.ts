import {
	IExecuteFunctions,
} from 'n8n-core';

import {
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';

import { ServiceBusClient, ServiceBusMessage } from '@azure/service-bus';

export class AzureMessageBusNode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Azure Message Bus Node',
		name: 'azureMessageBusNode',
		icon: 'file:azure.svg',
		group: ['transform'],
		version: 1,
		description: 'Sends and receives messages from Azure Service Bus',
		defaults: {
			name: 'Azure Service Bus',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			// Credenciais ou Connection String
			{
				displayName: 'Connection String',
				name: 'connectionString',
				type: 'string',
				default: '',
				required: true,
				description: 'Connection string para se autenticar no Azure Service Bus',
			},
			// Queue ou Topic
			{
				displayName: 'Queue or Topic Name',
				name: 'queueOrTopic',
				type: 'string',
				default: '',
				required: true,
				description: 'Nome da fila (queue) ou do tópico (topic) no Azure Service Bus',
			},
			// Subscription Name (apenas se for Topic)
			{
				displayName: 'Subscription Name (Se for Tópico)',
				name: 'subscriptionName',
				type: 'string',
				default: '',
				description: 'Nome da Subscription (caso esteja usando tópicos). Deixe vazio se for fila.',
			},
			// Operation: send ou receive
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Send',
						value: 'send',
					},
					{
						name: 'Receive',
						value: 'receive',
					},
				],
				default: 'send',
				description: 'Escolha a operação: enviar ou receber mensagens',
			},
			// Propriedade(s) específica(s) para a operação SEND
			{
				displayName: 'Message Body (JSON)',
				name: 'messageBody',
				type: 'json',   // <--- Importante para tratar como objeto
				default: {},    // Inicia como {}
				displayOptions: {
					show: {
						operation: [
							'send',
						],
					},
				},
				description: 'Conteúdo da mensagem a ser enviada em formato JSON',
			},
			// Propriedade(s) específica(s) para a operação RECEIVE
			{
				displayName: 'Maximum Number of Messages',
				name: 'maxMessages',
				type: 'number', // <--- Faz mais sentido ser number
				default: 1,
				displayOptions: {
					show: {
						operation: [
							'receive',
						],
					},
				},
				description: 'Quantidade máxima de mensagens para receber em uma única requisição',
			},
			{
				displayName: 'Max Wait Time (Ms)',
				name: 'maxWaitTime',
				type: 'number',
				default: 5000,
				displayOptions: {
					show: {
						operation: [
							'receive',
						],
					},
				},
				description: 'Tempo máximo (em milissegundos) para aguardar mensagens antes de encerrar a requisição',
			},
			{
				displayName: 'Post Processing Action',
				name: 'postProcess',
				type: 'options',
				displayOptions: {
					show: {
						operation: [
							'receive',
						],
					},
				},
				options: [
					{
						name: 'Complete (Remove Message)',
						value: 'complete',
						description: 'Automatically mark the message as complete',
					},
					{
						name: 'Abandon (Return to Queue)',
						value: 'abandon',
						description: 'Abandon message so it becomes available again in the queue',
					},
					{
						name: 'Dead-Letter',
						value: 'deadLetter',
						description: 'Move the message to the dead-letter queue',
					},
				],
				default: 'complete',
				description: 'What to do with the message after it is received',
			},
			{
				displayName: 'Receive Mode',
				name: 'receiveMode',
				type: 'options',
				displayOptions: {
					show: {
						operation: [
							'receive',
						],
					},
				},
				options: [
					{
						name: 'Receive And Complete',
						value: 'receiveAndComplete',
					},
					{
						name: 'Peek (Preview Only)',
						value: 'peek',
					},
				],
				default: 'receiveAndComplete',
				description: 'Whether to actually remove messages from the queue or just peek at them',
			},

		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const returnData: INodeExecutionData[] = [];

		// Obtenção dos parâmetros
		const connectionString = this.getNodeParameter('connectionString', 0) as string;
		const queueOrTopic = this.getNodeParameter('queueOrTopic', 0) as string;
		const subscriptionName = this.getNodeParameter('subscriptionName', 0, '') as string;
		const operation = this.getNodeParameter('operation', 0) as string;

		// Inicia o cliente de Service Bus
		const sbClient = new ServiceBusClient(connectionString);

		try {
			if (operation === 'send') {
				// "messageBody" já é do tipo "any" (objeto) pois definimos type: 'json'
				const messageBody = this.getNodeParameter('messageBody', 0);

				const sender = sbClient.createSender(queueOrTopic);
				// Envia o objeto diretamente no body
				const message: ServiceBusMessage = {
					body: messageBody,
				};
				await sender.sendMessages(message);
				await sender.close();

				// Retorna o objeto JSON, sem \n escapados
				returnData.push({
					json: {
						success: true,
						operation: 'send',
						[subscriptionName ? 'topic' : 'queueOrTopic']: queueOrTopic,
						subscriptionName,
						messageSent: messageBody,
					},
				});

			} else if (operation === 'receive') {
				const maxMessages = this.getNodeParameter('maxMessages', 0) as number;
				const maxWaitTime = this.getNodeParameter('maxWaitTime', 0) as number;

				const postProcess = this.getNodeParameter('postProcess', 0) as string;
				const receiveMode = this.getNodeParameter('receiveMode', 0) as string;

				let messages;
				if (receiveMode === 'peek') {
					// "Peek" não remove as mensagens da fila, apenas lê.
					if (!subscriptionName) {
						const receiver = sbClient.createReceiver(queueOrTopic);
						messages = await receiver.peekMessages(maxMessages);
						// Em peek, não chamamos "completeMessage", pois nada é “recebido” de fato.
						await receiver.close();
					} else {
						const receiver = sbClient.createReceiver(queueOrTopic, subscriptionName);
						messages = await receiver.peekMessages(maxMessages);
						await receiver.close();
					}
				} else {
					// "receiveAndComplete" (modelo atual)
					if (!subscriptionName) {
						const receiver = sbClient.createReceiver(queueOrTopic);
						messages = await receiver.receiveMessages(maxMessages, {
							maxWaitTimeInMs: maxWaitTime,
						});
						// Aqui você aplica "postProcess".
						for (const msg of messages) {
							if (postProcess === 'complete') {
								await receiver.completeMessage(msg);
							} else if (postProcess === 'abandon') {
								await receiver.abandonMessage(msg);
							} else if (postProcess === 'deadLetter') {
								await receiver.deadLetterMessage(msg);
							}
						}
						await receiver.close();
					} else {
						const receiver = sbClient.createReceiver(queueOrTopic, subscriptionName);
						messages = await receiver.receiveMessages(maxMessages, {
							maxWaitTimeInMs: maxWaitTime,
						});
						for (const msg of messages) {
							if (postProcess === 'complete') {
								await receiver.completeMessage(msg);
							} else if (postProcess === 'abandon') {
								await receiver.abandonMessage(msg);
							} else if (postProcess === 'deadLetter') {
								await receiver.deadLetterMessage(msg);
							}
						}
						await receiver.close();
					}
				}


				// messagesReceived: array contendo o "body" de cada mensagem
				returnData.push({
					json: {
						success: true,
						operation: 'receive',
						queueOrTopic,
						subscriptionName,
						messagesReceived: messages.map((m) => m.body),
					},
				});
			}
		} catch (error) {
			// Em caso de erro
			// eslint-disable-next-line n8n-nodes-base/node-execute-block-wrong-error-thrown
			throw new Error(`Erro ao executar a operação '${operation}': ${error.message}`);
		} finally {
			await sbClient.close();
		}

		return [returnData];
	}
}
