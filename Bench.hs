import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as B
import Data.Mercurial.Bundle
import System.Environment
import System.IO
import Debug.Trace

applyPatch orig (PatchAction start end dat) =
  B.concat [B.take (fromIntegral start) orig, dat, B.drop (fromIntegral end) orig]

applyPatches orig patch = foldl applyPatch orig $ pActions patch

main = do
  args <- getArgs
  bits <- B.readFile $ head args
  let changelog = cgChangelog $ parseBundleFile bits
  putStrLn $ BC.unpack $ BS.pack $ B.unpack $ foldl applyPatches B.empty $ take 30 changelog
