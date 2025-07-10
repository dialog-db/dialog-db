use dialog_common::{BLAKE3_HASH_SIZE, Blake3Hash};
use dialog_storage::{ContentAddressedStorage, DialogStorageError};

pub trait TreeStorage:
    ContentAddressedStorage<BLAKE3_HASH_SIZE, Hash = Blake3Hash, Error = DialogStorageError>
{
}

impl<Storage> TreeStorage for Storage where
    Storage:
        ContentAddressedStorage<BLAKE3_HASH_SIZE, Hash = Blake3Hash, Error = DialogStorageError>
{
}
