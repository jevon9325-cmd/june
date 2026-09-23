"""History observation identity, deliberately NOT economic event identity.

IG transactions v2 documents a reference but no unique row/event identifier or
revision lineage. Keep all exposed discriminators. Identical observations count
once for provisional arithmetic, while economic multiplicity remains unknown.
No caller-supplied 'verified' flag can promote this source to certified truth.
"""
from copy import deepcopy


def source_fields(row):
    # Preserve every source discriminator, including future/unknown fields.
    # The ledger canonicalizes known numeric/timestamp fields separately; these
    # remaining fields distinguish observations, not authenticated event IDs.
    normalized = {'openDateUtc', 'dateUtc', 'openLevel', 'closeLevel', 'size',
                  'profitAndLoss', 'currency', 'instrumentName', 'reference',
                  'transactionType'}
    return deepcopy({key: value for key, value in row.items() if key not in normalized})


IDENTITY_REASON = ('IG history provides observations, not proven unique economic '
                   'event IDs or revision lineage; multiplicity and ownership remain unresolved')
