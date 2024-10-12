import gleam/io

import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  io.println("Logs from your program will appear here!")

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(msg, state, conn) {
      io.println("Received message!")

      let assert glisten.Packet(req) = msg

      let assert <<
        req_len:size(32),
        api_key:size(16),
        api_version:size(16),
        correlation_id:size(32),
        _rest:bits,
      >> = req

      io.debug(req_len)
      io.debug(api_key)
      io.debug(api_version)
      io.debug(correlation_id)

      let error_code = case api_version {
        0 | 1 | 2 | 3 | 4 -> 0
        _ -> 35
      }

      let response =
        bytes_builder.concat_bit_arrays([
          <<correlation_id:big-size(32)>>,
          <<error_code:big-size(16)>>,
        ])

      let response = case api_key {
        18 -> api_versions(response)
        75 -> todo
        _ -> todo
      }

      let response =
        bytes_builder.prepend(response, <<
          bytes_builder.byte_size(response):big-size(32),
        >>)

      let assert Ok(_) = glisten.send(conn, response)

      actor.continue(state)
    })
    |> glisten.serve(9092)

  process.sleep_forever()
}

pub fn api_versions(
  response: bytes_builder.BytesBuilder,
) -> bytes_builder.BytesBuilder {
  bytes_builder.append_builder(
    response,
    bytes_builder.concat_bit_arrays([
      // arr length + 1
      <<3:big>>,
      <<18:big-size(16)>>,
      // min version 
      <<0:big-size(16)>>,
      // max
      <<4:big-size(16)>>,
      // tagged buffer 
      <<0>>,
      <<75:big-size(16)>>,
      // min version 
      <<0:big-size(16)>>,
      // max
      <<0:big-size(16)>>,
      // tagged buffer 
      <<0>>,
      // thottle time 
      <<0:size(32)>>,
      // tag buffer 
      <<0>>,
    ]),
  )
}
