module Lib.Agglomerator where

import           Streaming.Prelude (Stream, Of(..), yield, next)
import           Control.Monad.Trans.Class (lift)


data Particle x a = Slippery a | Sticky x | Unstick

agglomerate :: (Monoid x, Monad m) => Stream (Of (Particle x a)) m r -> Stream (Of (Either x a)) m r
agglomerate = go mempty 
    where
    go glob actions = lift (next actions) >>= \case
        Left r -> yield (Left glob) >> return r
        Right (particle, actions') -> case particle of
            Slippery a -> yield (Right a) >> go glob actions'
            Sticky x -> go (glob <> x) actions'
            Unstick -> yield (Left glob) >> go mempty actions'




