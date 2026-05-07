# Flows

## TCP Tunnel

```mermaid
sequenceDiagram
    participant App as Application
    participant C as Client
    participant B as MQTT Broker
    participant D as Device
    participant S as Target Service

    App->>C: TCP connect (local port)
    C->>B: open {mode: tcp, target: host:port}
    B->>D: open
    D->>S: TCP dial
    D->>B: open_ack {success: true}
    B->>C: open_ack

    loop Bidirectional Data
        App->>C: TCP data
        C->>B: data/{session_id} [seq|payload]
        B->>D: data/{session_id}
        D->>S: TCP data
        S->>D: TCP data
        D->>B: data/{session_id} [seq|payload]
        B->>C: data/{session_id}
        C->>App: TCP data
    end

    App->>C: TCP close
    C->>B: close
    B->>D: close
    D->>S: TCP close
```

## Shell Session

```mermaid
sequenceDiagram
    participant U as Terminal
    participant C as Client
    participant B as MQTT Broker
    participant D as Device
    participant P as PTY/Shell

    U->>C: stdin (raw mode)
    C->>B: open {mode: shell, cols: 80, rows: 24}
    B->>D: open
    D->>P: spawn $SHELL with PTY
    D->>B: open_ack {success: true}
    B->>C: open_ack

    loop Interactive I/O
        U->>C: keystrokes
        C->>B: data/{session_id}
        B->>D: data/{session_id}
        D->>P: write to PTY
        P->>D: read from PTY
        D->>B: data/{session_id}
        B->>C: data/{session_id}
        C->>U: terminal output
    end

    Note over C,D: On terminal resize (SIGWINCH)
    C->>B: resize {cols, rows}
    B->>D: resize
    D->>P: TIOCSWINSZ
```

## Command Execution

```mermaid
sequenceDiagram
    participant C as Client
    participant B as MQTT Broker
    participant D as Device

    C->>B: open {mode: exec, command: "uname -a"}
    B->>D: open
    D->>D: exec.CommandContext("sh", "-c", command)
    D->>B: open_ack {success: true}
    B->>C: open_ack

    loop Command Output
        D->>B: data/{session_id} [stdout]
        B->>C: data/{session_id}
    end

    D->>B: close {exit_code: 0}
    B->>C: close
```

## Ping

```mermaid
sequenceDiagram
    participant C as Client
    participant B as MQTT Broker
    participant D as Device

    loop N times
        C->>B: ping {timestamp}
        B->>D: ping
        D->>B: pong {timestamp}
        B->>C: pong
        C->>C: RTT = now - timestamp
    end

    C->>C: Print statistics (min/avg/max, packet loss)
```

## Reconnection

```mermaid
stateDiagram-v2
    [*] --> Connected
    Connected --> ConnectionLost: broker disconnect
    ConnectionLost --> Reconnecting: auto-reconnect
    Reconnecting --> Connected: success
    Reconnecting --> Reconnecting: retry
    ConnectionLost --> SessionCleanup: close all sessions
    SessionCleanup --> Reconnecting
    Connected --> SubscriptionsRestored: resubscribe
```

On connection loss, all active sessions are torn down. The MQTT client reconnects automatically with unlimited retries. Subscriptions are restored on reconnect via the broker-side session or client-side resubscription.
