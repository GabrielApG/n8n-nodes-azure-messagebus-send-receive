import {
	IExecuteFunctions,
} from 'n8n-core';

import {
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
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
			{
				displayName: 'Connection String',
				name: 'connectionString',
				type: 'string',
				default: '',
				required: true,
				description: 'Connection string para se autenticar no Azure Service Bus',
			},
			{
				displayName: 'Queue or Topic Name',
				name: 'queueOrTopic',
				type: 'string',
				default: '',
				required: true,
				description: 'Nome da fila (queue) ou do tópico (topic) no Azure Service Bus',
			},
			{
				displayName: 'Subscription Name (Se for Tópico)',
				name: 'subscriptionName',
				type: 'string',
				default: '',
				description: 'Nome da Subscription (caso esteja usando tópicos). Deixe vazio se for fila.',
			},
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
			{
				displayName: 'Message Body (JSON)',
				name: 'messageBody',
				type: 'json',
				default: {},
				displayOptions: {
					show: {
						operation: [
							'send',
						],
					},
				},
				description: 'Conteúdo da mensagem a ser enviada em formato JSON',
			},
			{
				displayName: 'Maximum Number of Messages',
				name: 'maxMessages',
				type: 'number',
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
					},
					{
						name: 'Abandon (Return to Queue)',
						value: 'abandon',
					},
					{
						name: 'Dead-Letter',
						value: 'deadLetter',
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
				const items = this.getInputData(); // Obtém todos os itens de entrada
				const sender = sbClient.createSender(queueOrTopic);

				for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
					try {
						// Obtém os dados para a mensagem atual
						const messageBody = this.getNodeParameter('messageBody', itemIndex) as any;

						// Prepara a mensagem
						const message: ServiceBusMessage = {
							body: messageBody,
						};

						// Envia a mensagem
						await sender.sendMessages(message);

						// Adiciona o resultado de sucesso ao retorno
						returnData.push({
							json: {
								success: true,
								itemIndex,
								messageSent: messageBody,
							},
						});
					} catch (error) {
						// Lida com erros individuais
						if (this.continueOnFail()) {
							returnData.push({
								json: {
									success: false,
									itemIndex,
									error: error.message,
								},
							});
						} else {
							throw new NodeOperationError(this.getNode(), error, { itemIndex });
						}
					}
				}

				// Fecha o sender após enviar todas as mensagens
				await sender.close();
			} else if (operation === 'receive') {
				const maxMessages = this.getNodeParameter('maxMessages', 0) as number;
				const maxWaitTime = this.getNodeParameter('maxWaitTime', 0) as number;
				const postProcess = this.getNodeParameter('postProcess', 0) as string;
				const receiveMode = this.getNodeParameter('receiveMode', 0) as string;
				let messages;
				if (receiveMode === 'peek') {
					const receiver = subscriptionName
						? sbClient.createReceiver(queueOrTopic, subscriptionName)
						: sbClient.createReceiver(queueOrTopic);
					messages = await receiver.peekMessages(maxMessages);
					await receiver.close();
				} else {
					const receiver = subscriptionName
						? sbClient.createReceiver(queueOrTopic, subscriptionName)
						: sbClient.createReceiver(queueOrTopic);
					messages = await receiver.receiveMessages(maxMessages, { maxWaitTimeInMs: maxWaitTime });
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
			throw new NodeOperationError(this.getNode(), `Erro ao executar a operação '${operation}': ${error.message}`);
		} finally {
			await sbClient.close();
		}

		return [returnData];
	}
}
