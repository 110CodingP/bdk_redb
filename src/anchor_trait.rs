use bdk_wallet::chain::{Anchor, BlockId, ConfirmationBlockTime};
use redb::Value;
pub trait AnchorWithMetaData: Anchor {
    type MetaDataType: Value + 'static;

    // to be used as value in anchors table
    fn metadata(&self) -> <Self::MetaDataType as redb::Value>::SelfType<'_>;

    // to use in read_anchors
    fn from_id(id: BlockId, metadata: <Self::MetaDataType as redb::Value>::SelfType<'_>) -> Self;
}

impl AnchorWithMetaData for ConfirmationBlockTime {
    type MetaDataType = u64;

    fn metadata(&self) -> <Self::MetaDataType as redb::Value>::SelfType<'_> {
        self.confirmation_time
    }

    fn from_id(id: BlockId, metadata: <Self::MetaDataType as redb::Value>::SelfType<'_>) -> Self {
        ConfirmationBlockTime {
            block_id: id,
            confirmation_time: metadata,
        }
    }
}

impl AnchorWithMetaData for BlockId {
    type MetaDataType = Option<()>;

    fn metadata(&self) -> <Self::MetaDataType as redb::Value>::SelfType<'_> {
        None
    }

    fn from_id(id: BlockId, metadata: <Self::MetaDataType as redb::Value>::SelfType<'_>) -> Self {
        let _ = metadata;
        id
    }
}
