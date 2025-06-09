use bdk_wallet::bitcoin::{self, Amount, BlockHash, ScriptBuf, Txid, hashes::Hash};
use bdk_wallet::chain::{BlockId, DescriptorId};
use redb::{Key, TypeName, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ScriptWrapper(pub(crate) ScriptBuf);

impl Value for ScriptWrapper {
    type SelfType<'a> = ScriptWrapper;
    type AsBytes<'a> = Vec<u8>;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.clone().into_bytes()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        ScriptWrapper(ScriptBuf::from_bytes(data.to_vec()))
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("tx_graph")
    }
}

impl Key for ScriptWrapper {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let vec1 = data1.to_vec();
        let vec2 = data2.to_vec();
        vec1[0].cmp(&vec2[0])
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TransactionWrapper(pub(crate) bitcoin::Transaction);
impl Value for TransactionWrapper {
    type SelfType<'a> = TransactionWrapper;
    type AsBytes<'a> = Vec<u8>;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut vec: Vec<u8> = Vec::new();
        ciborium::into_writer(value, &mut vec).unwrap();
        vec
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        ciborium::from_reader(data).unwrap()
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("transaction")
    }
}

impl Key for TransactionWrapper {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let tx1: TransactionWrapper = ciborium::from_reader(data1).unwrap();
        let tx2: TransactionWrapper = ciborium::from_reader(data2).unwrap();
        tx1.0.version.cmp(&tx2.0.version)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlockHashWrapper(pub(crate) BlockHash);

impl Value for BlockHashWrapper {
    type SelfType<'a> = BlockHashWrapper;
    type AsBytes<'a> = [u8; 32];
    fn fixed_width() -> Option<usize> {
        Some(32usize)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_byte_array()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        BlockHashWrapper(BlockHash::from_slice(data).unwrap())
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("block_hash")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlockIdWrapper(pub(crate) BlockId);

impl Value for BlockIdWrapper {
    type SelfType<'a> = BlockIdWrapper;
    type AsBytes<'a> = [u8; 36];
    fn fixed_width() -> Option<usize> {
        Some(36usize)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut bytes: [u8; 36] = [0; 36];
        bytes[0..4].copy_from_slice(&value.0.height.to_le_bytes());
        bytes[4..].copy_from_slice(&value.0.hash.to_byte_array());
        bytes
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let bytes: [u8; 36] = data.try_into().unwrap();
        let block_id = BlockId {
            height: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            hash: BlockHash::from_slice(&bytes[4..]).unwrap(),
        };
        BlockIdWrapper(block_id)
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("block_id")
    }
}

impl Key for BlockIdWrapper {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DIDWrapper(pub(crate) DescriptorId);
impl Value for DIDWrapper {
    type SelfType<'a> = DIDWrapper;
    type AsBytes<'a> = [u8; 32];
    fn fixed_width() -> Option<usize> {
        Some(32usize)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_byte_array()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        DIDWrapper(DescriptorId::from_slice(data).unwrap())
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("descriptor_id")
    }
}

impl Key for DIDWrapper {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TxidWrapper(pub(crate) Txid);

impl Value for TxidWrapper {
    type SelfType<'a> = TxidWrapper;
    type AsBytes<'a> = [u8; 32];
    fn fixed_width() -> Option<usize> {
        Some(32usize)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_byte_array()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        TxidWrapper(Txid::from_slice(data).unwrap())
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("txid")
    }
}

impl Key for TxidWrapper {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AmountWrapper(pub(crate) Amount);

impl Value for AmountWrapper {
    type SelfType<'a> = AmountWrapper;
    type AsBytes<'a> = [u8; 8];
    fn fixed_width() -> Option<usize> {
        Some(32usize)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_sat().to_le_bytes()
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        AmountWrapper(Amount::from_sat(u64::from_le_bytes(
            data.try_into().unwrap(),
        )))
    }
    fn type_name() -> redb::TypeName {
        TypeName::new("txid")
    }
}
