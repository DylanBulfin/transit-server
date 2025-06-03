use std::marker::PhantomData;

use prost::Message;
use tonic::codec::{BufferSettings, Codec, ProstCodec};

pub mod diff;
pub mod error;
pub mod log;
pub mod shared;

pub struct LargeBufferCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Codec for LargeBufferCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = <ProstCodec<T, U> as Codec>::Encoder;
    type Decoder = <ProstCodec<T, U> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProstCodec::<T, U>::raw_encoder(BufferSettings::new(1024 * 1024, 8192))
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstCodec::<T, U>::raw_decoder(BufferSettings::default())
    }
}
