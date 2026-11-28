# SIMD string kernels

Native Calc uses SIMD byte search and transformation for compatible string inputs. Search still
returns Flink's 1-based character positions. Unicode inputs use UTF-8 character boundaries to
interpret start positions and translate byte offsets. Binary inputs with malformed UTF-8 continue
to use Flink's byte stepping rules.

`LOCATE`, `POSITION`, and `INSTR` use the existing `memchr` dependency for byte search.
A literal needle prepares its substring finder once per batch, following DataFusion's
`strpos` pattern. Short and mixed-text searches can check candidate offsets directly with
`memchr`. Backward `INSTR` checks the last byte of each candidate with `memrchr`. Both paths
bound the number of rejected candidates before switching to general substring search. `INSTR`
retains overlapping matches. `SPLIT_INDEX` uses SIMD single-byte separator search and reuses
the finder for a literal multi-byte separator. It consumes only the separator matches needed
for the requested token.
The empty string separator still uses Flink's Java whitespace rules.

ASCII `LOWER` and `UPPER` map bytes directly into the final Arrow value buffer. The represented
case path also maps cached ASCII units when the worker's locale rules permit it. `REVERSE` writes
each row in reverse into its final byte span, preserving the original offsets and null validity.
The compiler vectorizes the ASCII byte loops. On x86-64 with AVX2, case conversion uses a
wider loop for buffers of at least 256 bytes. On Unicode rows, reversal preserves complete code
points and the JDK-compatible case path applies the worker's existing locale and cache rules.

## Release measurements

Run the scalar group on prepared Arrow inputs. [Native Criterion methodology](../benchmarks/native-criterion.md)

```sh
python3 bin/bench-native.py --bench scalar_functions --filter string_simd
```

The release comparison used Rust 1.95.0 on one logical CPU of an Intel Core i7-12650H.
Each invocation processed 4,096 Arrow rows with result allocation included. The nominal
prefix widths were 12, 256, and 2,048 bytes, followed by short suffixes with hits and misses.
The 256-byte Unicode fixture had one NULL every eight rows. The prior implementation came
from `71dbaf767`. Both implementations produced equivalent Arrow outputs for all 44 fixtures.
The table shows the range of prior-to-current median latency ratios from two alternating pairs.

| Operation | Short ASCII | 256-byte ASCII | 256-byte Unicode with NULLs | Long ASCII |
| --- | ---: | ---: | ---: | ---: |
| `LOCATE` with a column needle | 1.32–1.33× | 1.23× | 1.71× | 1.02–1.03× |
| `POSITION` with a literal needle | 0.99–1.01× | 1.01× | 0.96–1.00× | 0.99–1.01× |
| `POSITION` with a column needle | 1.25–1.26× | 2.25–2.26× | 1.02× | 3.80–4.20× |
| Forward `INSTR` with a literal needle | 2.96× | 2.12–2.25× | 1.81–1.86× | 0.98–1.00× |
| Backward `INSTR` with a column needle | 1.29–1.31× | 1.76–1.77× | 1.75–1.76× | 1.82–1.87× |
| `SPLIT_INDEX` with a literal substring | 1.56–1.57× | 3.97–4.02× | 2.85× | 6.42–6.52× |
| `SPLIT_INDEX` with a literal byte | 2.94–3.03× | 2.46× | 15.25–15.35× | 2.58–2.93× |
| `SPLIT_INDEX` with a column separator | 1.14–1.15× | 1.40× | 1.68–1.69× | 1.59–1.60× |
| `LOWER` | 1.00–1.27× | 1.03–1.04× | 0.99–1.00× | 1.05–1.07× |
| `UPPER` | 1.25–1.29× | 0.96–1.02× | 0.99× | 1.04–1.05× |
| `REVERSE` | 1.42–1.45× | 1.59–1.98× | 1.07–1.10× | 1.22–1.30× |

Literal `POSITION` continues to use DataFusion's prepared finder. The Unicode column-needle
fixture continues to use its variable-needle path. The large single-byte split result for
Unicode comes from a delimiter miss. Case results measure the direct UTF-8 registration.
These timings cover kernel execution on prepared Arrow buffers.
