%%% @private
%%% Internal module handling HTTP(S) requests setup to the avro registry.
%%% Handles TLS validation and credentials handling, because until
%%% later versions of OTP this is kinda cumbersome to handle.
%%%
%%% The code is based off code authors have written in Rebar3 to handle
%%% package manager authentication.
%%% @end
-module(avrogato_httpc).
-include_lib("public_key/include/OTP-PUB-KEY.hrl"). % cert validation
-export([opts/1, request/3]).
-define(HTTPC_TIMEOUT, 10000).

-spec opts(string()) -> {httpc:headers(), httpc:http_options()}.
opts(URI) ->
    case uri_string:parse(URI) of
        #{scheme := "https", host := HostName, userinfo := UInfo} ->
            {auth_headers(UInfo), base_opts() ++ ssl_opts(HostName)};
        #{scheme := "https", host := HostName} ->
            {[], base_opts() ++ ssl_opts(HostName)};
        #{scheme := "http", userinfo := UInfo} ->
            {auth_headers(UInfo), base_opts()};
        #{scheme := "http"} ->
            {[], base_opts()}
    end.

-spec request(string(), httpc:headers(), httpc:http_options()) -> binary().
request(URI, Headers, Opts) ->
    case httpc:request(get, {URI, Headers}, Opts, []) of
        {ok, {{_Vsn, Code, _}, _Hdrs, Body}} when Code >= 200, Code < 300 ->
            unicode:characters_to_binary(Body);
        {ok, {{_Vsn, Code, Reason}, _Hdrs, _Body}} ->
            error({bad_http_response, {Code, Reason}});
        Other ->
            error({bad_http_response, Other})
    end.

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%
base_opts() ->
    [{timeout, ?HTTPC_TIMEOUT}, {relaxed, true}].

ssl_opts(HostName) ->
    VerifyFun = {fun ssl_verify_hostname:verify_fun/3,
                 [{check_hostname, HostName}]},
    CACerts = certifi:cacerts(),
    [{ssl, [{verify, verify_peer}, {depth, 2}, {cacerts, CACerts},
            {partial_chain, fun partial_chain/1}, {verify_fun, VerifyFun}]}].

auth_headers(UserInfo) ->
    [{"Authorization", "Basic " ++ base64:encode_to_string(UserInfo)}].

-spec partial_chain(Certs) -> Res when
      Certs :: list(any()),
      Res :: unknown_ca | {trusted_ca, any()}.
partial_chain(Certs) ->
    Certs1 = [{Cert, public_key:pkix_decode_cert(Cert, otp)} || Cert <- Certs],
    CACerts = certifi:cacerts(),
    CACerts1 = [public_key:pkix_decode_cert(Cert, otp) || Cert <- CACerts],
    case lists:search(fun({_, Cert}) -> check_cert(CACerts1, Cert) end, Certs1) of
        {value, Trusted} ->
            {trusted_ca, element(1, Trusted)};
        _ ->
            unknown_ca
    end.

-spec check_cert(CACerts, Cert) -> Res when
      CACerts :: list(any()),
      Cert :: any(),
      Res :: boolean().
check_cert(CACerts, Cert) ->
    lists:any(fun(CACert) ->
                extract_public_key_info(CACert) == extract_public_key_info(Cert)
              end, CACerts).

-spec extract_public_key_info(Cert) -> Res when
      Cert :: #'OTPCertificate'{tbsCertificate::#'OTPTBSCertificate'{}},
      Res :: any().
extract_public_key_info(Cert) ->
    ((Cert#'OTPCertificate'.tbsCertificate)#'OTPTBSCertificate'.subjectPublicKeyInfo).
