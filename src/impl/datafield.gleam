import gleam/bit_array
import gleam/bytes_builder as bb
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string

// See: https://kafka.apache.org/protocol.html#protocol_types
pub type DataField {
  Kint(value: Int, size: Int, zigzag: Bool)
  Kstring(value: Option(String), compact: Bool, nullable: Bool)
  Kuuid(value: String)
  Kboolean(value: Bool)
  TaggedFields
}

pub fn serialize_data_field(d: DataField) -> BitArray {
  case d {
    TaggedFields -> <<0>>
    Kboolean(False) -> <<0>>
    Kboolean(True) -> <<1>>
    Kint(value, size, False) -> <<value:big-size(size)>>
    Kint(value, _, True) -> zigzag_encode(value)
    Kuuid(value) -> bb.to_bit_array(bb.from_string(value))
    // STRING 
    Kstring(value, False, False) -> {
      let value = option.unwrap(value, "")
      bb.to_bit_array(
        bb.prepend(bb.from_string(value), <<
          string.byte_size(value):big-size(16),
        >>),
      )
    }
    // COMPACT_STRING 
    Kstring(value, False, True) -> {
      let value = option.unwrap(value, "")
      bb.to_bit_array(bb.prepend(
        bb.from_string(value),
        zigzag_encode(string.byte_size(value)),
      ))
    }
    // NULLABLE_STRING 
    Kstring(value, True, False) -> {
      case value {
        Some(value) -> {
          bb.to_bit_array(
            bb.prepend(bb.from_string(value), <<
              string.byte_size(value):big-size(16),
            >>),
          )
        }
        None -> <<-1:big-size(16)>>
      }
    }
    // COMPACT_NULLABLE_STRING 
    Kstring(value, True, True) -> {
      case value {
        Some(value) ->
          bb.to_bit_array(bb.prepend(
            bb.from_string(value),
            zigzag_encode(string.byte_size(value)),
          ))
        None -> <<0>>
      }
    }
  }
}

pub fn serialize_list(l: List(DataField)) -> BitArray {
  // length is + 1  
  let length = list.length(l) + 1

  let acc = bb.from_bit_array(<<length:big-size(32)>>)

  let result = list.fold(l, acc, append_data)

  bb.to_bit_array(result)
}

fn append_data(acc: bb.BytesBuilder, d: DataField) -> bb.BytesBuilder {
  bb.append(acc, serialize_data_field(d))
}

pub fn zigzag_decode(val: BitArray) -> #(Int, BitArray) {
  io.debug(bit_array.inspect(val))
  let #(bits, rest) = zigzag_decode_helper(<<>>, val)
  // for each byte in the input, there was 1 extra continuation bit. 
  let num_bits = bit_array.byte_size(bits) * 8 - bit_array.byte_size(bits)
  let assert <<i:size(num_bits)>> = bits
  #(i, rest)
}

fn zigzag_decode_helper(acc: BitArray, data: BitArray) -> #(BitArray, BitArray) {
  case data {
    <<0:size(1), val:size(7), rest:bits>> -> #(
      bit_array.append(<<val:size(7)>>, acc),
      rest,
    )
    <<1:size(1), val:size(7), rest:bits>> ->
      zigzag_decode_helper(bit_array.append(<<val:size(7)>>, acc), rest)
    _ -> panic
    // eeeee
  }
}

fn zigzag_helper(acc: bb.BytesBuilder, n: Int) -> bb.BytesBuilder {
  case n <= 0x7f {
    True -> bb.append(acc, <<int.bitwise_and(n, 0x7F)>>)
    False -> {
      let acc =
        bb.append(acc, <<int.bitwise_or(int.bitwise_and(n, 0x7F), 0x80)>>)
      let n = int.bitwise_shift_right(n, 7)
      zigzag_helper(acc, n)
    }
  }
}

pub fn zigzag_encode(i: Int) -> BitArray {
  bb.to_bit_array(zigzag_helper(bb.new(), i))
}
