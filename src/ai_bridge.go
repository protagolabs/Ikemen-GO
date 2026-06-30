// ai_bridge.go
// M1 验证：通过 Unix Socket 暴露引擎控制接口，供外部 AI 进程驱动对战
// 支持三个命令：GET_STATE / SET_INPUT / STEP
// 放入 Ikemen-GO/src/ 目录即可编译，无需修改其他文件（system.go 除外，见注释）

package main

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
)

// -----------------------------------------------------------------------
// 通信数据结构
// -----------------------------------------------------------------------

// BridgeCmd：外部进程发来的指令
type BridgeCmd struct {
	Cmd    string `json:"cmd"`             // GET_STATE | SET_INPUT | STEP | PAUSE
	Player int    `json:"player"`          // 0=P1, 1=P2（SET_INPUT 时使用）
	Input  string `json:"input,omitempty"` // "forward+a" 等组合，见下方解析
	Frames int    `json:"frames,omitempty"` // STEP 推进的帧数，默认 30
}

// GameState：返回给外部进程的游戏状态快照
type GameState struct {
	P1HP    int32   `json:"p1_hp"`
	P1HPMax int32   `json:"p1_hp_max"`
	P1X     float32 `json:"p1_x"`
	P1Y     float32 `json:"p1_y"`
	P1State int32   `json:"p1_state"` // 当前状态机编号
	P2HP    int32   `json:"p2_hp"`
	P2HPMax int32   `json:"p2_hp_max"`
	P2X     float32 `json:"p2_x"`
	P2Y     float32 `json:"p2_y"`
	P2State int32   `json:"p2_state"`
	Frame   int32   `json:"frame"`   // 当前游戏帧
	Paused  bool    `json:"paused"`
	Round   int     `json:"round"`
}

// BridgeResp：返回给外部进程的响应
type BridgeResp struct {
	OK    bool        `json:"ok"`
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

// -----------------------------------------------------------------------
// 内部通信 channel（bridge goroutine ↔ 游戏主循环）
// -----------------------------------------------------------------------

var (
	bridgeCmdCh  = make(chan BridgeCmd)   // 外部指令 → 主循环
	bridgeRespCh = make(chan BridgeResp)  // 主循环 → 外部响应

	// stepRemain：剩余需要推进的帧数；原子操作，主循环每帧 -1
	stepRemain int32
)

// -----------------------------------------------------------------------
// InitAiBridge：在引擎启动时调用一次（在 system.go 的 main() 或 New() 中）
// -----------------------------------------------------------------------

func InitAiBridge() {
	sys.bridgeInput = &BridgeInput{}
	sockPath := os.Getenv("IKEMEN_BRIDGE_SOCK")
	if sockPath == "" {
		sockPath = "/tmp/ikemen_bridge.sock"
	}
	os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		log.Printf("[ai_bridge] 启动失败: %v", err)
		return
	}

	log.Printf("[ai_bridge] 监听 %s", sockPath)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			// 每个连接串行处理（格斗 AI 指令不需要并发）
			go handleConn(conn)
		}
	}()
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil || n == 0 {
		return
	}

	var cmd BridgeCmd
	if err := json.Unmarshal(buf[:n], &cmd); err != nil {
		resp, _ := json.Marshal(BridgeResp{OK: false, Error: "invalid json"})
		conn.Write(append(resp, '\n'))
		return
	}

	// 发给主循环处理，等待响应
	bridgeCmdCh <- cmd
	resp := <-bridgeRespCh

	out, _ := json.Marshal(resp)
	conn.Write(append(out, '\n'))
}

// -----------------------------------------------------------------------
// ProcessBridgeCmd：在游戏主循环每帧调用一次（非阻塞）
// 在 system.go 的 fight() 循环体最前面加：sys.ProcessBridgeCmd()
// -----------------------------------------------------------------------

func (s *System) ProcessBridgeCmd() {
	// 如果有剩余步进帧，倒计时
	if remain := atomic.LoadInt32(&stepRemain); remain > 0 {
		atomic.AddInt32(&stepRemain, -1)
		if atomic.LoadInt32(&stepRemain) == 0 {
			s.paused = true // 步进完成后自动暂停
		}
		return
	}

	// 非阻塞读取外部指令
	select {
	case cmd := <-bridgeCmdCh:
		s.execBridgeCmd(cmd)
	default:
	}
}

func (s *System) execBridgeCmd(cmd BridgeCmd) {
	switch strings.ToUpper(cmd.Cmd) {

	case "GET_STATE":
		bridgeRespCh <- BridgeResp{OK: true, Data: s.buildGameState()}

	case "PAUSE":
		s.paused = true
		bridgeRespCh <- BridgeResp{OK: true}

	case "SET_INPUT":
		idx := cmd.Player
		if idx < 0 || idx >= len(s.bridgeInput.ib) {
			bridgeRespCh <- BridgeResp{OK: false, Error: "invalid player index"}
			return
		}
		var facing int32 = 1
		if idx < len(s.chars) && len(s.chars[idx]) > 0 && s.chars[idx][0].facing < 0 {
			facing = -1
		}
		s.bridgeInput.ib[idx] = moveToInputBits(cmd.Input, facing)
		bridgeRespCh <- BridgeResp{OK: true}

	case "STEP":
		frames := cmd.Frames
		if frames <= 0 {
			frames = 30
		}
		atomic.StoreInt32(&stepRemain, int32(frames))
		s.paused = false
		bridgeRespCh <- BridgeResp{OK: true}

	default:
		bridgeRespCh <- BridgeResp{OK: false, Error: "unknown command: " + cmd.Cmd}
	}
}

// -----------------------------------------------------------------------
// buildGameState：从引擎读取当前帧的游戏状态
// -----------------------------------------------------------------------

func (s *System) buildGameState() GameState {
	gs := GameState{
		Frame:  int32(s.tickCount),
		Paused: s.paused,
		Round:  int(s.round),
	}

	if len(s.chars) > 0 && len(s.chars[0]) > 0 {
		c := s.chars[0][0]
		gs.P1HP = c.life
		gs.P1HPMax = c.lifeMax
		gs.P1X = c.pos[0]
		gs.P1Y = c.pos[1]
		gs.P1State = c.ss.no
	}

	if len(s.chars) > 1 && len(s.chars[1]) > 0 {
		c := s.chars[1][0]
		gs.P2HP = c.life
		gs.P2HPMax = c.lifeMax
		gs.P2X = c.pos[0]
		gs.P2Y = c.pos[1]
		gs.P2State = c.ss.no
	}

	return gs
}

// moveToInputBits converts a bridge action string (engine tokens like
// "forward", "forward+a", "c", "none") into an InputBits bitmask. forward/back
// are resolved to physical L/R against facing; GetInput later re-derives B/F.
func moveToInputBits(input string, facing int32) InputBits {
	var ib InputBits
	fwd, back := IB_PR, IB_PL // facing right: forward=physical right
	if facing < 0 {
		fwd, back = IB_PL, IB_PR
	}
	for _, p := range strings.Split(strings.ToLower(input), "+") {
		switch strings.TrimSpace(p) {
		case "forward", "f":
			ib |= fwd
		case "back":
			ib |= back
		case "up", "u", "jump":
			ib |= IB_PU
		case "down", "d", "crouch":
			ib |= IB_PD
		case "downforward", "df":
			ib |= IB_PD | fwd
		case "downback", "db":
			ib |= IB_PD | back
		case "upforward", "uf":
			ib |= IB_PU | fwd
		case "upback", "ub":
			ib |= IB_PU | back
		case "a", "lp", "light_punch":
			ib |= IB_A
		case "b", "lk", "light_kick":
			ib |= IB_B
		case "c", "hp", "heavy_punch":
			ib |= IB_C
		case "x", "hk", "heavy_kick":
			ib |= IB_X
		case "y":
			ib |= IB_Y
		case "z":
			ib |= IB_Z
		case "s", "start":
			ib |= IB_S
		}
	}
	return ib
}
