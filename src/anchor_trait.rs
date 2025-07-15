//! This module contains [`AnchorWithMetaData`] trait and implementations of the trait for some
//! types.
use bdk_chain::{Anchor, BlockId, ConfirmationBlockTime};
use redb::Value;

/// A trait that provides metadata corresponding to an [`Anchor`].
///
/// [`Anchor`]: <https://docs.rs/bdk_chain/0.23.0/bdk_chain/trait.Anchor.html>
/// In case of ConfirmationBlockTime the metadata will be the confirmation time while in case of
/// BlockId it will simply be None.
pub trait AnchorWithMetaData: Anchor {
    /// Type corresponding to the Anchor's metadata.
    type MetaDataType: Value + 'static;

    /// This function returns the metadata corresponding to the anchor.
    fn metadata(&self) -> <Self::MetaDataType as redb::Value>::SelfType<'_>;

    /// This function creates an Anchor from BlockId and metadata.
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
