# Learning Pipeline Demo Tests

Ниже два теста, которые лучше всего показывают, что каскадный mapping pipeline в проекте реально используется и полезен.

## Прогон

Команда:

```bash
python3 -m unittest \
  mvp_backend.test_learning_pipeline.LearningPipelineTests.test_identifier_match_resolves_before_llm_when_graph_and_rules_are_available \
  mvp_backend.test_learning_pipeline.LearningPipelineTests.test_conflicting_semantics_stay_unresolved_below_final_threshold
```

Результат:

```txt
..
----------------------------------------------------------------------
Ran 2 tests in 0.052s

OK
```

## 1. Identifier Match Resolves Before LLM

Тест:
- [test_learning_pipeline.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_learning_pipeline.py#L206)

Что проверяет:
- в storage заранее сохраняются подтверждённые связи:
  - `user_id -> accountId`
  - `id -> user_id`
- затем pipeline получает вход:
  - source columns: `id`, `comment`
  - target field: `accountId`

Что важно:
- `rank_mapping_candidate` вообще не вызывается
- значит pipeline не идёт в модель
- он использует уже накопленные знания и/или deterministic match раньше LLM

Что это доказывает:
- система действительно каскадная
- она не тратит токены на очевидный кейс
- память и ранние этапы pipeline реально работают

## 2. Conflicting Semantics Stay Unresolved Below Final Threshold

Тест:
- [test_learning_pipeline.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_learning_pipeline.py#L173)

Что проверяет:
- модель предлагает сомнительное соответствие:
  - target: `dealCreationDate`
  - source: `dealUpdateDate`
- pipeline не принимает это автоматически

Что важно:
- `mapping.source == None`
- `mapping.source_of_truth == "unresolved"`
- включается semantic conflict:
  - `semantic_conflict_created_vs_updated`
- итоговая уверенность падает ниже acceptance threshold

Что это доказывает:
- pipeline полезен не только для нахождения соответствий
- он ещё и защищает от ложных маппингов
- система не превращается в “LLM сказал, значит принимаем”

## Почему именно эти два теста хороши для защиты

Вместе они показывают два ключевых свойства проекта:

1. При хорошем сигнале система обходит LLM и экономит токены.
2. При плохом сигнале система не пускает ошибочный match дальше.

То есть они доказывают, что каскадный pipeline:
- реально используется
- снижает зависимость от модели
- уменьшает расход токенов
- повышает надёжность генерации








mapping: accountId <- id
LLM call: no
source_of_truth: deterministic_rule / semantic_graph



target: dealCreationDate
model candidate: dealUpdateDate
raw model confidence: 0.74


mapping: unresolved
source: null
rejection_reason: semantic_conflict_created_vs_updated
final_confidence < acceptance_threshold


