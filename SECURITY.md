# Política de Segurança

## Reportando Vulnerabilidades

**Nunca** abra uma issue pública para reportar vulnerabilidades de segurança.

Use o [GitHub Security Advisories](https://github.com/OWNER/testa-de-ferro/security/advisories/new) para reportar de forma privada.

### O que reportar

- SQL injection (DuckDB)
- Cross-Site Scripting (XSS)
- Vazamento de CPFs (texto claro em logs, respostas, .duckdb sem HMAC)
- Bypass de rate limiting
- Path traversal em endpoints de exportação
- Exposição de stack traces ou informação interna da API
- Dependências com CVEs críticas não corrigidas
- Qualquer forma de acesso não autorizado a dados

### O que NÃO é vulnerabilidade

- Dados públicos acessíveis pela API (nomes, CNPJs, contratos — são dados abertos por definição)
- Sugestões de melhoria sem vetor de ataque concreto
- Bugs funcionais sem implicação de segurança

## Tempo de Resposta

| Etapa           | Prazo    |
| --------------- | -------- |
| Triagem inicial | 72 horas |
| Confirmação     | 7 dias   |
| Correção        | 30 dias  |

Vulnerabilidades críticas (vazamento de CPF, SQL injection) são priorizadas acima de qualquer feature.

## Divulgação

Seguimos divulgação coordenada: o reporter é creditado publicamente após a correção, a menos que prefira anonimato.
