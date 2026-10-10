from server.genie import _research_prompt, _sanitize_individual_answer


def test_research_prompt_preserves_question_and_requires_complete_governed_answer():
    question = "¿Cuál fue el total de inversiones del último trimestre?"

    prompt = _research_prompt(question)

    assert question in prompt
    assert "mesmo idioma" in prompt
    assert "período, unidade, moeda e escopo" in prompt
    assert "decomposição mais relevante" in prompt
    assert "Não invente valores" in prompt


def test_individual_answer_hides_party_id_and_raw_customer_table():
    question = "Analiza el cliente con party_id = QPTY-000000086702."
    answer = """## Cliente QPTY-000000086702

Resumen seguro.

**Perfil completo Customer 360 del cliente QPTY-000000086702**

| party_id | risk_band |
| --- | --- |
| QPTY-000000086702 | low |

*Showing 2 of 22 columns*

**Aviso legal:** datos sintéticos.
"""
    sanitized = _sanitize_individual_answer(question, answer)
    assert "QPTY-" not in sanitized
    assert "| party_id" not in sanitized
    assert "Perfil completo" not in sanitized
    assert "Resumen seguro" in sanitized
    assert "Aviso legal" in sanitized
