mod error;
use error::MissingError;
use redb::{
    Database, ReadTransaction, TableDefinition,  Value,
    WriteTransaction,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;

pub trait Storable<'a>:'a +  Serialize + Deserialize<'a> + Value<SelfType<'a> = Self, AsBytes<'a> = Vec<u8>> {

    fn merge(&mut self, other: &Self::SelfType<'a> );

    fn default() -> Self::SelfType<'a>;
}

#[derive(Debug, thiserror::Error)]
pub enum BdkRedbError {
    #[error(transparent)]
    RedbError(#[from] redb::Error),

    #[error(transparent)]
    DataMissingError(#[from] MissingError),
}

pub struct Store<C> {
    db: Database,
    wallet_name: String,
    _data: PhantomData<C>,
}

impl<C> Store<C> where C:'static +  Storable<'static>  {//confused so static 

    const TABLE: TableDefinition<'static, &'static str, C> = TableDefinition::new("table");
    pub fn load_or_create<P>(file_path: P, wallet_name: String) -> Result<Self, BdkRedbError>
    where
        P: AsRef<Path>,
    {
        let db = Database::create(file_path).map_err(redb::Error::from)?;
        Ok(Store::<C> { db, wallet_name, _data: PhantomData::<C> })
    }

    pub fn persist(
        &self,
        db_tx: &WriteTransaction,
        changeset: &C,
    ) -> Result<(), BdkRedbError> {
        let mut table = db_tx.open_table(Self::TABLE).map_err(redb::Error::from)?;
        let mut aggregated_changeset: C = match table.remove(&*self.wallet_name).unwrap() {
            Some(value) => match value.value() {
                changeset => changeset,
            },
            None => C::default(),
        };
        <C as Storable>::merge(&mut aggregated_changeset, changeset.clone());
        table
            .insert(
                &*self.wallet_name,
                aggregated_changeset,
            )
            .unwrap();
        Ok(())
    }

    pub fn read(
        &self,
        db_tx: &ReadTransaction,
        changeset: &mut C,
    ) -> Result<(), BdkRedbError> {
        let table = db_tx.open_table(Self::TABLE).map_err(redb::Error::from)?;
        *changeset =
            table.get(&*self.wallet_name).unwrap().unwrap().value();
        Ok(())
    }

}