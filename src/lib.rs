use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

#[cfg(test)]
test_r::enable!();

pub type BoxStream<A> = Box<dyn Stream<Item = A>>;

#[async_trait]
pub trait Stream: Send + Sync + 'static {
    type Item;

    async fn uncons(self: Box<Self>) -> Option<(Self::Item, BoxStream<Self::Item>)>;

    fn boxed(self) -> Box<Self>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub enum StreamImpl<A: Send + Sync + 'static> {
    Empty,
    Emit(A),
    EmitAsync(
        Box<dyn (FnOnce() -> Pin<Box<dyn Future<Output = A> + Send>>) + Send + Sync + 'static>,
    ),
    Flatten(BoxStream<BoxStream<A>>),
    Concat(
        BoxStream<A>,
        Box<dyn (FnOnce() -> BoxStream<A>) + Send + Sync + 'static>,
    ),
}

#[async_trait]
impl<A: Send + Sync + 'static> Stream for StreamImpl<A> {
    type Item = A;

    async fn uncons(self: Box<Self>) -> Option<(Self::Item, BoxStream<Self::Item>)> {
        match *self {
            Self::Empty => None,
            Self::Emit(value) => Some((value, empty())),
            Self::EmitAsync(future) => {
                let value = future().await;
                Some((value, empty()))
            }
            Self::Flatten(stream) => {
                let (current_stream, rest) = stream.uncons().await?;
                match current_stream.uncons().await {
                    None => flatten(rest).uncons().await,
                    Some((value, inner_rest)) => {
                        let rest = cons(inner_rest, rest);
                        let rest = flatten(rest);
                        Some((value, rest))
                    }
                }
            }
            Self::Concat(stream, next) => match stream.uncons().await {
                None => next().uncons().await,
                Some((value, rest)) => Some((value, concat(rest, next()))),
            },
        }
    }
}

pub struct PrependedStream<A: Send + Sync + 'static> {
    value: A,
    rest: BoxStream<A>,
}

#[async_trait]
impl<A: Send + Sync + 'static> Stream for PrependedStream<A> {
    type Item = A;

    async fn uncons(self: Box<Self>) -> Option<(Self::Item, BoxStream<Self::Item>)> {
        Some((self.value, self.rest))
    }
}

// Stream API

pub fn empty<A: Send + Sync + 'static>() -> BoxStream<A> {
    StreamImpl::Empty.boxed()
    // EmptyStream { _a: PhantomData }.boxed()
}

pub fn singleton<A: Send + Sync + 'static>(value: A) -> BoxStream<A> {
    StreamImpl::Emit(value).boxed()
}

pub fn emit<A: Send + Sync + 'static, F>(f: F) -> BoxStream<A>
where
    F: (FnOnce() -> Pin<Box<dyn Future<Output = A> + Send>>) + Send + Sync + 'static,
{
    StreamImpl::EmitAsync(Box::new(f)).boxed()
}

pub fn cons<A: Send + Sync + 'static>(value: A, rest: Box<dyn Stream<Item = A>>) -> BoxStream<A> {
    PrependedStream { value, rest }.boxed()
}

pub fn concat<A: Send + Sync + 'static>(
    stream1: BoxStream<A>,
    stream2: BoxStream<A>,
) -> BoxStream<A> {
    StreamImpl::Concat(stream1, Box::new(move || stream2)).boxed()
}

pub fn flatten<A: Send + Sync + 'static>(stream: BoxStream<BoxStream<A>>) -> BoxStream<A> {
    StreamImpl::Flatten(stream).boxed()
}

#[async_trait]
trait StreamOps<A: Send + Sync + 'static> {
    async fn drain_foreach<F>(self, f: F)
    where
        F: (FnMut(A) -> ()) + Send + Sync + 'static;

    fn map<B: Send + Sync + 'static, F>(self, f: F) -> BoxStream<B>
    where
        F: (Fn(A) -> B) + Send + Sync + 'static;

    fn map_async<B: Send + Sync + 'static, F>(self, f: F) -> BoxStream<B>
    where
        F: Clone + (Fn(A) -> Pin<Box<dyn Future<Output = B> + Send>>) + Send + Sync + 'static;

    fn prepend(self, value: A) -> BoxStream<A>;
}

#[async_trait]
impl<A: Send + Sync + 'static> StreamOps<A> for BoxStream<A> {
    async fn drain_foreach<F>(self, f: F)
    where
        F: (FnMut(A) -> ()) + Send + Sync + 'static,
    {
        internal::perform_drain_foreach(self, f).await
    }

    fn map<B: Send + Sync + 'static, F>(self, f: F) -> BoxStream<B>
    where
        F: (Fn(A) -> B) + Send + Sync + 'static,
    {
        flatten(emit(move || {
            Box::pin(async move { internal::perform_map(self, f).await })
        }))
    }

    fn map_async<B: Send + Sync + 'static, F>(self, f: F) -> BoxStream<B>
    where
        F: Clone + Fn(A) -> Pin<Box<dyn Future<Output = B> + Send>> + Send + Sync + 'static,
    {
        flatten(emit(move || {
            Box::pin(async move { internal::perform_map_async(self, f).await })
        }))
    }

    fn prepend(self, value: A) -> BoxStream<A> {
        cons(value, self)
    }
}

mod internal {
    use crate::{concat, cons, emit, empty, BoxStream};
    use async_recursion::async_recursion;
    use std::future::Future;
    use std::pin::Pin;

    #[async_recursion]
    pub async fn perform_map<A: Send + Sync + 'static, B: Send + Sync + 'static, F>(
        stream: BoxStream<A>,
        f: F,
    ) -> BoxStream<B>
    where
        F: (Fn(A) -> B) + Send + Sync + 'static,
    {
        match stream.uncons().await {
            None => empty(),
            Some((value, rest)) => cons(f(value), perform_map(rest, f).await),
        }
    }

    #[async_recursion]
    pub async fn perform_map_async<A: Send + Sync + 'static, B: Send + Sync + 'static, F>(
        stream: BoxStream<A>,
        f: F,
    ) -> BoxStream<B>
    where
        F: Clone + (Fn(A) -> Pin<Box<dyn Future<Output = B> + Send>>) + Send + Sync + 'static,
    {
        let f2 = f.clone();
        match stream.uncons().await {
            None => empty(),
            Some((value, rest)) => concat(
                emit(move || Box::pin(async move { f2(value).await })),
                perform_map_async(rest, f).await,
            ),
        }
    }

    #[async_recursion]
    pub async fn perform_drain_foreach<Item, F>(stream: BoxStream<Item>, mut f: F)
    where
        Item: Send + 'static,
        F: (FnMut(Item) -> ()) + Send + Sync,
    {
        match stream.uncons().await {
            None => {}
            Some((value, rest)) => {
                f(value);
                perform_drain_foreach(rest, f).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_r::test;

    #[test]
    async fn it_works() {
        let stream1 = cons(1, cons(2, cons(3, empty())));
        stream1.drain_foreach(|value| println!("{}", value)).await;

        async fn delayed_emit(n: i32) -> i32 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            n
        }

        let stream2 = concat(
            emit(|| Box::pin(delayed_emit(1))),
            concat(
                emit(|| Box::pin(delayed_emit(2))),
                emit(|| Box::pin(delayed_emit(3))),
            ),
        );
        stream2.drain_foreach(|value| println!("{}", value)).await;

        let stream3 = empty()
            .prepend(3)
            .prepend(2)
            .prepend(1)
            .map(|n| n * 2)
            .map(|n| n + 1);

        stream3.drain_foreach(|value| println!("{}", value)).await;

        let stream4 = cons(1, cons(2, cons(3, empty()))).map_async(|n| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                n * 2
            })
        });

        stream4.drain_foreach(|value| println!("{}", value)).await;
    }
}

/*
Notes:

- Can't be enum Stream<A> because recursion leads to 'error[E0320]: overflow while adding drop-check rules for Stream<A>'
    => so we use Box<dyn Stream<Item=A>>
- Introduced Emit vs EmitAsync for simplicity, but it's not needed
- Problem in uncons implementation - can't construct Stream(Stream) because it leads to 'reached the recursion limit while instantiating' error
    => Separate PrependedStream type as a workaround for 'reached the recursion limit while instantiating'
- How to make cloneable streams while not making it mandatory?
 */
