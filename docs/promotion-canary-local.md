# Runtime do Canary — alterações locais

O Worker continua usando a mesma fila e o mesmo processador idempotente. Novo código local ainda não foi publicado/deployado.

`controlled_consumer.poll_once` usa MaxNumberOfMessages1, long poll20s, visibility180s e ApproximateReceiveCount. Métricas contam sucesso somente após ack; falha de ack é distinta de erro de persistência. Mensagem inválida/falha não é apagada. Drain antes do receive não inicia consumo; drain ocorrido durante long poll devolve a mensagem ainda não processada com visibility0. Mensagem já em processamento termina antes do drain.

Stable usa RELEASE_CHANNEL=stable e versão por digest. Canary usa RELEASE_CHANNEL=canary e exige WORKER_CONTROL_FILE com JSON mode=run e expires_at válido, no máximo300s no futuro. Ausência, expiração, JSON inválido ou modo diferente de run impedem novos polls. A renovação de lease pelo coordenador ainda não está implantada. Sem ela o Canary permanece parado por desenho.

Health/ready falha durante drain; liveness permanece saudável enquanto drena/processa para não provocar reinício deliberado no meio de uma mensagem. Se processamento travar, o controlador não deve matar o pod para forçar rollback: alertar e investigar. Falhas de nó/Spot e expiração SQS continuam possíveis; idempotência protege os efeitos simulados e a reserva de estoque transacional. Não há garantia de exactly-once de entrega SQS.

`/metrics` expõe mensagens por resultado, retries, erros DynamoDB, erros de polling, histograma de duração, inflight, draining e heartbeat, com labels version/channel controlados. Não expor /metrics no Ingress público. Não incluir payloads, IDs de pedido ou receipt handles nos labels.

O bundle é gerado por cosmotech-gitops/scripts/render_worker_canary.py; dashboard e PodMonitor estão no repo observability. Antes do Canary real é necessário um baseline stable com essa instrumentação, aprovado como release própria. Imagem antiga sem métricas não serve de baseline comparável.

Os testes usam mocks/moto e cobrem duplicidade entre instâncias stable/canary sem repetir a reserva de estoque. Isso não valida o runtime EKS. Não foi adicionado mecanismo de falha artificial ao Worker de produção; a falha do demo é injetada apenas nos dados de teste.


## Revisão Bedrock / publisher

Modelo regional Nova Micro escolhido no adaptador Converse, sem chamada paga. Publisher GPG, portal WSGI autenticado, SNS existente e adaptadores de solicitação/execução foram preparados. CIs locais só solicitam; permissões/workflows remotos não foram alterados. A avaliação `.disabled` não é o fluxo principal completo. Coletor confiável, bindings de staging/reconciliação e ativação continuam pendentes; não habilitar enforcement_verified sem prova independente. Relatório consolidado em `/opt/aws-serverless-lab/docs/linkedin-showcase/10-action-plan.md`.
