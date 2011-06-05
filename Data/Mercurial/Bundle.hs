module Data.Mercurial.Bundle
       (
         ChunkHeader(..)
       , Chunk(..)
       , Patch(..)
       , PatchAction(..)
       , Group
       , PatchGroup
       , Changegroup(..)
       , parseBundleFile
       ) where

import qualified Codec.Compression.BZip as BZ2
import qualified Codec.Compression.GZip as GZ
import Control.Monad
import Data.Binary.Get
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString as BS
import Data.Word
import Debug.Trace

data ChunkHeader = ChunkHeader { chNode :: BS.ByteString
                               , chParent1 :: BS.ByteString
                               , chParent2 :: BS.ByteString
                               , chChangeset :: BS.ByteString }
                 deriving (Show)

data Chunk = Chunk { cHeader :: ChunkHeader
                   , cData :: BS.ByteString }
             deriving (Show)

data Patch = Patch { pHeader :: ChunkHeader
                   , pActions :: [PatchAction] }
           deriving (Show)

data PatchAction = PatchAction { paStart :: Word32 
                               , paEnd :: Word32
                               , paData :: B.ByteString }
                 deriving (Show)

type Group = [Chunk]

type PatchGroup = [Patch]

data Changegroup = Changegroup { cgChangelog :: PatchGroup
                               , cgManifest :: Group
                               , cgFiles :: [(String, PatchGroup)] }
                 deriving (Show)

parseBundleFile :: B.ByteString -> Changegroup
parseBundleFile dat = 
  let contents = runGet getBundleContents dat
  in runGet getChangegroup contents

getBundleContents :: Get B.ByteString
getBundleContents = do
  magic <- getWord32be
  unless (magic == 0x48473130) $ fail "Invalid bundle header."
  comp <- lookAhead getWord16be
  case comp of
    0x425A -> fmap BZ2.decompress getRemainingLazyByteString
    0x475A -> fmap GZ.decompress getRemainingLazyByteString
    0x554E -> skip 2 >> getRemainingLazyByteString
    _ -> error "Unknown bundle compression type."

getChangegroup :: Get Changegroup
getChangegroup = do
  changelog <- getPatchGroup
  manifest <- getNormalGroup
  files <- getFileList
  return $ Changegroup { cgChangelog = changelog
                       , cgManifest = manifest
                       , cgFiles = files }

getWhile :: Get (Bool, a) -> (a -> Get b) -> Get [b]
getWhile getCond getElem = fmap reverse $ helper getCond getElem []
  where
    helper getCond getElem res  = do
      (continue, closure) <- getCond
      if continue
        then getElem closure >>= (\x -> helper getCond getElem (x:res))
        else return res

getNormalGroup :: Get Group
getNormalGroup = getWhile cond getChunk
  where cond = do
          len <- getWord32be
          return (len >= 84, len)

getPatchGroup :: Get PatchGroup
getPatchGroup = getWhile cond getPatch
  where cond = do
          len <- getWord32be
          return (len >= 84, len)

getFileList :: Get [(String, PatchGroup)]
getFileList = getWhile cond getFile
  where cond = do
          len <- getWord32be
          return (len >= 4, len)

getChunkHeader :: Get ChunkHeader
getChunkHeader = do
  node <- getByteString 20
  p1 <- getByteString 20
  p2 <- getByteString 20
  cs <- getByteString 20
  return $ ChunkHeader { chNode = node
                       , chParent1 = p1
                       , chParent2 = p2
                       , chChangeset = cs }

getChunk :: Word32 -> Get Chunk
getChunk len = do
  header <- getChunkHeader
  dat <- getByteString (fromIntegral len - 84)
  return $ Chunk { cHeader = header
                 , cData = dat }

getPatch :: Word32 -> Get Patch
getPatch len = do
  header <- getChunkHeader
  at <- bytesRead
  let end = fromIntegral at + len - 84
  actions <- getWhile (cond end) (\_ -> getPatchAction)
  return $ Patch { pHeader = header
                 , pActions = actions }
  where
    cond end = do
      at <- bytesRead
      return (fromIntegral at < end, ())

getPatchAction :: Get PatchAction
getPatchAction = do
  start <- getWord32be
  end <- getWord32be
  dataLen <- getWord32be
  dat <- getLazyByteString $ fromIntegral dataLen
  return $ PatchAction { paStart = start
                       , paEnd = end
                       , paData = dat }

getFile :: Word32 -> Get (String, PatchGroup)
getFile len = do
  filenameBytes <- getByteString $ fromIntegral len - 4
  let filename = BC.unpack filenameBytes
  group <- getPatchGroup
  return $ (filename, group)
