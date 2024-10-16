import gleam/bit_array
import gleam/int
import gleam/io
import gleeunit
import gleeunit/should
import impl/datafield

pub fn main() {
  gleeunit.main()
}

pub fn var_int_encode_test() {
  let assert <<result:big-size(16)>> = datafield.zigzag_encode(150)
  should.equal(result, 0x9601)
}

pub fn var_int_decode_test() {
  let assert #(result, _body) = datafield.zigzag_decode(<<38_401:size(16)>>)
  io.debug(int.to_base2(result))
  should.equal(result, 150)
}
