# cosmotech-orders-worker

SQS consumer extracted from cosmotech-orders-api with original Git ancestry.
Shared Orders modules and contract tests are retained as a pinned snapshot until a
versioned shared package is introduced. This repository owns Dockerfile.worker and
the worker runtime; API source remains only for shared contract tests.

CI/CD: push main → lint/tests → Docker build → Trivy → ECR via short-lived OIDC →
GitHub App image-only GitOps PR → optional DEV auto-promotion → Argo CD.
Actions never patches EKS. PRs only test/build/scan and cannot publish.

DEV delivery is enabled without human approval. ECR_PUBLISH_ROLE_ARN selects the
worker-only OIDC role; GITOPS_APP_ID and GITOPS_APP_PRIVATE_KEY use the existing
GitHub App installed only on cosmotech-gitops. DEV_AUTO_PROMOTE=true enables the
validated image-only merge. Missing configuration fails delivery rather than
silently skipping publication. Main source and workflow changes trigger CI.

Host: https://orders-worker-dev.cosmotechsolutions.com.br/health/ready.
Only health GETs are served; submit business requests to orders-api-dev, which queues
SQS messages. Readiness requires a successful poll and clears on queue errors,
shutdown or 90 seconds without progress. Liveness also detects a stalled loop.
HPA: 1–4 replicas, worker container CPU target 70%, 300-second downscale stabilization.
CPU scaling is a baseline; SQS backlog scaling requires an external metrics adapter.
Existing idempotency, bounded SDK retries, 180-second visibility and queue DLQ are
preserved. No queue/table migration or new application data retention is introduced.

On release check Argo Synced/Healthy, HPA AbleToScale/ScalingActive, endpoint TLS,
queue age/DLQ and processing errors. Queue-age/DLQ alert ownership stays with the
existing observability stack. Roll back by reverting the GitOps image digest; never
reset queue contents or DynamoDB inventory. Extra replicas can increase EKS capacity
and AWS request costs. CI image artifacts retain two days; promotion evidence uses
the GitHub artifact default retention.

## Configuração e entrega DEV

`deploy/dev` neste repositório é a origem oficial de Deployment, ConfigMap, Service, HPA e host. Um push em `main` executa testes, valida essas configurações, constrói e verifica a imagem, publica no ECR e promove automaticamente o conjunto completo para o espelho gerado de entrega GitOps. Argo CD reconcilia o cluster e Kestra mostra as etapas reais por cluster. Não há aprovação humana nesta fase. Não edite o espelho gerado para configurar esta aplicação.
