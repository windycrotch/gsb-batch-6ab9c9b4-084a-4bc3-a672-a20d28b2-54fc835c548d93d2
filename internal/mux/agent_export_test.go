// Copyright 2026 The Oto Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mux

// AgentCtrlArrivals reports how many serialized control calls have taken an
// arrival ticket. It is intended for tests only.
func (p *Player) AgentCtrlArrivals() uint64 {
	return p.p.ctrlArrival.Load()
}

// AgentCtrlServed reports the arrival ticket of the next control call allowed
// to run: the number of control calls that have fully handed off their turn.
// It is intended for tests only.
func (p *Player) AgentCtrlServed() uint64 {
	p.p.m.Lock()
	defer p.p.m.Unlock()
	return p.p.ctrlServed
}

// AgentReading reports whether a source Read is currently in flight.
// It is intended for tests only.
func (p *Player) AgentReading() bool {
	p.p.m.Lock()
	defer p.p.m.Unlock()
	return p.p.reading
}

// AgentSeeking reports whether a Seek section is active.
// It is intended for tests only.
func (p *Player) AgentSeeking() bool {
	p.p.m.Lock()
	defer p.p.m.Unlock()
	return p.p.seeking
}
