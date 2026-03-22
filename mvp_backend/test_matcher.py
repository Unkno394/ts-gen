from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    import pydantic  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    pydantic_stub = types.ModuleType('pydantic')

    class _FieldInfo:
        def __init__(self, default=None, default_factory=None):
            self.default = default
            self.default_factory = default_factory

    def Field(default=None, default_factory=None):
        return _FieldInfo(default=default, default_factory=default_factory)

    class BaseModel:
        def __init__(self, **data):
            annotations = {}
            for cls in reversed(self.__class__.mro()):
                annotations.update(getattr(cls, '__annotations__', {}))
            for key in annotations:
                if key in data:
                    value = data[key]
                else:
                    class_value = getattr(self.__class__, key, None)
                    if isinstance(class_value, _FieldInfo):
                        if class_value.default_factory is not None:
                            value = class_value.default_factory()
                        else:
                            value = class_value.default
                    else:
                        value = class_value
                setattr(self, key, value)

        def dict(self):
            return dict(self.__dict__)

        def model_dump(self):
            return dict(self.__dict__)

        def copy(self, update=None):
            payload = dict(self.__dict__)
            payload.update(update or {})
            return self.__class__(**payload)

        def model_copy(self, update=None):
            return self.copy(update=update)

    pydantic_stub.BaseModel = BaseModel
    pydantic_stub.Field = Field
    sys.modules['pydantic'] = pydantic_stub

from matcher import map_fields, prepare_field_name
from models import TargetField


class MatcherTests(unittest.TestCase):
    def test_uses_column_order_fallback_when_nothing_matches(self) -> None:
        target_fields = [
            TargetField(name='customerName', type='string'),
            TargetField(name='amount', type='number'),
            TargetField(name='createdAt', type='string'),
        ]

        mappings, warnings = map_fields(['1223', 'hsdh', 'sdvsdv'], target_fields)

        self.assertEqual([mapping.source for mapping in mappings], ['1223', 'hsdh', 'sdvsdv'])
        self.assertTrue(all(mapping.confidence == 'low' for mapping in mappings))
        self.assertTrue(all(mapping.reason == 'position_fallback' for mapping in mappings))
        self.assertEqual(
            warnings,
            ['No semantic column matches found. Used column-order fallback because source and target have the same number of fields.'],
        )

    def test_keeps_semantic_matches_without_forcing_order_fallback(self) -> None:
        target_fields = [
            TargetField(name='customerName', type='string'),
            TargetField(name='amount', type='number'),
        ]

        mappings, warnings = map_fields(['customer_name', 'zzz'], target_fields)

        self.assertEqual(mappings[0].source, 'customer_name')
        self.assertEqual(mappings[0].confidence, 'high')
        self.assertIsNone(mappings[1].source)
        self.assertEqual(mappings[1].confidence, 'none')
        self.assertIn('No source column found for target "amount"', warnings)

    def test_preprocessing_exposes_canonical_tokens_for_ru_en_semantics(self) -> None:
        prepared = prepare_field_name('ФИО клиента')
        self.assertIn('name', prepared['canonical_tokens'])
        self.assertIn('customer', prepared['canonical_tokens'])

    def test_prepare_field_name_extracts_entity_attribute_and_role(self) -> None:
        user_id = prepare_field_name('user_id')
        product_id = prepare_field_name('productId')
        plain_id = prepare_field_name('id')

        self.assertEqual(user_id['entity_token'], 'user')
        self.assertEqual(user_id['attribute_token'], 'id')
        self.assertEqual(user_id['role_label'], 'identifier')

        self.assertEqual(product_id['entity_token'], 'product')
        self.assertEqual(product_id['attribute_token'], 'id')
        self.assertEqual(product_id['role_label'], 'identifier')

        self.assertIsNone(plain_id['entity_token'])
        self.assertEqual(plain_id['attribute_token'], 'id')
        self.assertEqual(plain_id['role_label'], 'identifier')

    def test_maps_crm_semantic_columns_with_ambiguous_id_and_reason_tokens(self) -> None:
        target_fields = [
            TargetField(name='actPlanDate', type='string'),
            TargetField(name='closeReason', type='string'),
            TargetField(name='closeReasonComment', type='string'),
            TargetField(name='creator', type='string'),
            TargetField(name='dealId', type='string'),
            TargetField(name='dealIdentifier', type='string'),
            TargetField(name='dealLastUpdateDate', type='string'),
            TargetField(name='dealRevenueAmount', type='number'),
        ]

        mappings, warnings = map_fields(
            [
                'Плановая дата акта',
                'Сделка - Причина закрытия',
                'Сделка - Комментарий к причине закрытия',
                'Сделка - Создал',
                'Сделка - ID сделки',
                'Сделка - Идентификатор',
                'Сделка - Дата последнего обновления',
                'Сделка - Сумма выручки',
            ],
            target_fields,
            allow_position_fallback=False,
        )

        mapping_by_target = {mapping.target: mapping for mapping in mappings}
        self.assertEqual(mapping_by_target['actPlanDate'].source, 'Плановая дата акта')
        self.assertEqual(mapping_by_target['closeReason'].source, 'Сделка - Причина закрытия')
        self.assertEqual(mapping_by_target['closeReasonComment'].source, 'Сделка - Комментарий к причине закрытия')
        self.assertEqual(mapping_by_target['creator'].source, 'Сделка - Создал')
        self.assertEqual(mapping_by_target['dealId'].source, 'Сделка - ID сделки')
        self.assertEqual(mapping_by_target['dealIdentifier'].source, 'Сделка - Идентификатор')
        self.assertEqual(mapping_by_target['dealLastUpdateDate'].source, 'Сделка - Дата последнего обновления')
        self.assertEqual(mapping_by_target['dealRevenueAmount'].source, 'Сделка - Сумма выручки')
        self.assertEqual(warnings, [])

    def test_maps_extended_crm_fields_without_null_source_fallbacks(self) -> None:
        target_fields = [
            TargetField(name='dealStage', type='string'),
            TargetField(name='dealStageFinal', type='boolean'),
            TargetField(name='dealStageTransitionDate', type='string'),
            TargetField(name='deliveryType', type='string'),
            TargetField(name='directSupply', type='boolean'),
            TargetField(name='distributor', type='string'),
            TargetField(name='finalLicenseAmount', type='number'),
            TargetField(name='finalServiceAmount', type='number'),
            TargetField(name='finalServiceAmountByRevenueWithVAT', type='number'),
            TargetField(name='finalServiceAmountWithVAT', type='number'),
            TargetField(name='forecast', type='string'),
            TargetField(name='invoiceAmount', type='number'),
            TargetField(name='invoiceAmountWithVAT', type='number'),
            TargetField(name='marketingEvent', type='string'),
            TargetField(name='organization', type='string'),
            TargetField(name='responsiblePerson', type='string'),
            TargetField(name='siteLead', type='boolean'),
            TargetField(name='stageTransitionTime', type='string'),
            TargetField(name='totalProductAmount', type='number'),
            TargetField(name='unitOfMeasure', type='string'),
        ]

        mappings, warnings = map_fields(
            [
                'Сделка - Стадия',
                'Стадия (Сделка)',
                'Сделка - Дата перехода объекта на новую стадию',
                'Тип поставки',
                'Сделка - Прямая поставка',
                'Сделка - Дистрибьютор',
                'Сделка - Итоговая сумма лицензий',
                'Сделка - Итоговая сумма услуг',
                'Сделка - Итоговая сумма услуг по выручке (с НДС)',
                'Сделка - Итоговая сумма услуг (с НДС)',
                'Сделка - Прогноз',
                'Сумма акта',
                'Сумма акта (с НДС)',
                'Сделка - Маркетинговое мероприятие',
                'Сделка - Организация',
                'Сделка - Ответственный',
                'Сделка - Лид с сайта',
                'Время перехода на текущую стадию',
                'Сделка - Итоговая сумма продуктов',
                'Единица измерения',
            ],
            target_fields,
            allow_position_fallback=False,
        )

        mapping_by_target = {mapping.target: mapping for mapping in mappings}
        self.assertEqual(mapping_by_target['dealStage'].source, 'Сделка - Стадия')
        self.assertEqual(mapping_by_target['dealStageFinal'].source, 'Стадия (Сделка)')
        self.assertEqual(mapping_by_target['dealStageTransitionDate'].source, 'Сделка - Дата перехода объекта на новую стадию')
        self.assertEqual(mapping_by_target['deliveryType'].source, 'Тип поставки')
        self.assertEqual(mapping_by_target['directSupply'].source, 'Сделка - Прямая поставка')
        self.assertEqual(mapping_by_target['distributor'].source, 'Сделка - Дистрибьютор')
        self.assertEqual(mapping_by_target['finalLicenseAmount'].source, 'Сделка - Итоговая сумма лицензий')
        self.assertEqual(mapping_by_target['finalServiceAmount'].source, 'Сделка - Итоговая сумма услуг')
        self.assertEqual(mapping_by_target['finalServiceAmountByRevenueWithVAT'].source, 'Сделка - Итоговая сумма услуг по выручке (с НДС)')
        self.assertEqual(mapping_by_target['finalServiceAmountWithVAT'].source, 'Сделка - Итоговая сумма услуг (с НДС)')
        self.assertEqual(mapping_by_target['forecast'].source, 'Сделка - Прогноз')
        self.assertEqual(mapping_by_target['invoiceAmount'].source, 'Сумма акта')
        self.assertEqual(mapping_by_target['invoiceAmountWithVAT'].source, 'Сумма акта (с НДС)')
        self.assertEqual(mapping_by_target['marketingEvent'].source, 'Сделка - Маркетинговое мероприятие')
        self.assertEqual(mapping_by_target['organization'].source, 'Сделка - Организация')
        self.assertEqual(mapping_by_target['responsiblePerson'].source, 'Сделка - Ответственный')
        self.assertEqual(mapping_by_target['siteLead'].source, 'Сделка - Лид с сайта')
        self.assertEqual(mapping_by_target['stageTransitionTime'].source, 'Время перехода на текущую стадию')
        self.assertEqual(mapping_by_target['totalProductAmount'].source, 'Сделка - Итоговая сумма продуктов')
        self.assertEqual(mapping_by_target['unitOfMeasure'].source, 'Единица измерения')
        self.assertEqual(warnings, [])

    def test_maps_operation_code_and_name_columns_without_position_fallback(self) -> None:
        target_fields = [
            TargetField(name='code', type='string'),
            TargetField(name='description', type='string'),
        ]

        mappings, warnings = map_fields(
            [
                'Наименование видов операций',
                'Код вида операций',
            ],
            target_fields,
            allow_position_fallback=False,
        )

        mapping_by_target = {mapping.target: mapping for mapping in mappings}
        self.assertEqual(mapping_by_target['code'].source, 'Код вида операций')
        self.assertEqual(mapping_by_target['description'].source, 'Наименование видов операций')
        self.assertIn(mapping_by_target['code'].confidence, {'high', 'medium'})
        self.assertIn(mapping_by_target['description'].confidence, {'high', 'medium'})
        self.assertEqual(warnings, [])


if __name__ == '__main__':
    unittest.main()
