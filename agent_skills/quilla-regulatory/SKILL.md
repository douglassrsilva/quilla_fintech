---
name: quilla-regulatory
description: Recupera e resume o corpus regulatório sintético Quilla com filtros obrigatórios de país, idioma e citações.
version: 1.0.0
---

# Quilla RegIntel

1. Fixe `country_code` e idioma a partir do contexto autenticado.
2. Consulte exclusivamente o MCP de AI Search do índice `regulatory_knowledge_hybrid_idx`.
3. Exija `is_official=false`, `synthetic_flag=true` e correspondência exata de país.
4. Cite IDs QREF e separe fatos recuperados, inferências e recomendações.
5. Aponte lacunas e encaminhe interpretações para jurídico/compliance local.

Nunca apresente o corpus como fonte oficial, combine jurisdições silenciosamente ou dê parecer jurídico definitivo.
