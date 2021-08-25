import "server_info.dart";

/// Options for establishing server connection
class ConnectionOptions {
  /// Turns on +OK protocol acknowledgements
  bool? verbose;

  /// Turns on additional strict format checking, e.g. for properly formed subjects
  bool? pedantic;

  /// Indicates whether the client requires an SSL connection
  bool? tlsRequired;

  /// Client authorization token (if [ServerInfo.authRequired] is set)
  String? authToken;

  /// Connection username (if [ServerInfo.authRequired] is set)
  String? userName;

  /// Connection password (if [ServerInfo.authRequired] is set)
  String? password;

  /// Optional client name
  String? name;

  /// Language implementation of the client
  String? language;

  /// Version of the client
  String? version;

  /// Sending `0` (or absent) indicates client supports original protocol. Sending `1` indicates that the client supports dynamic reconfiguration of cluster topology changes by asynchronously receiving `INFO` messages with known servers it can reconnect to.
  int? protocol;

  /// If set to [true], the server (version 1.2.0+) will not send originating messages from this connection to its own subscriptions. Clients should set this to true only for server supporting this feature, which is when [protocol] in the `INFO` protocol is set to at least `1`
  bool? echo;
}
