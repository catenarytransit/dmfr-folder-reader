# Read a dmfr folder and get a list of corrosponding data via hashmaps

## Usage
Put the transitland-atlas folder somewhere.

```rust
 let dmfr_result = read_folders("transitland-atlas/feeds");
 ```

## Test
```bash
cargo test -- --nocapture
```