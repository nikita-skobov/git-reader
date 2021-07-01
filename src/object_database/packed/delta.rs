use std::io;
use crate::ioerre;

/// No clue how this works to be honest.
/// I copied it directly from:
/// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L235
/// I couldnt wrap my head around this.
pub fn apply_delta(
    base_data: &[u8],
    delta_data: &[u8],
    output_len: usize
) -> io::Result<Vec<u8>> {
    let mut output = unsafe {
        let mut out = Vec::with_capacity(output_len);
        out.set_len(output_len);
        out
    };

    let delta_len = delta_data.len();

    let mut result_pos = 0;
    let mut base_pos;
    let mut index = 0;
    while index < delta_len {
        let mut opcode = delta_data[index];
        index += 1;

        if opcode & 0x80 > 0 {
            // copy from base to dest
            let mut copy_offset = 0;
            let mut copy_len = 0;
            let mut shift = 0;
            for _ in 0..4 {
                if opcode & 0x01 > 0 {
                    copy_offset |= (delta_data[index] as usize) << shift;
                    index += 1;
                }
                opcode >>= 1;
                shift += 8;
            }

            shift = 0;
            for _ in 0..3 {
                if opcode & 0x01 > 0 {
                    copy_len |= (delta_data[index] as usize) << shift;
                    index += 1;
                }
                opcode >>= 1;
                shift += 8;
            }

            if copy_len == 0 {
                copy_len = 1 << 16;
            }
            base_pos = copy_offset;
            for _ in 0..copy_len {
                output[result_pos] = base_data[base_pos];
                result_pos += 1;
                base_pos += 1;
            }
        } else if opcode > 0 {
            // insert n bytes at the end:
            for _ in 0..(opcode as usize) {
                output[result_pos] = delta_data[index];
                result_pos += 1;
                index += 1;
            }
        } else {
            return ioerre!("Error, opcode should not be 0");
        }
    }

    Ok(output)
}
