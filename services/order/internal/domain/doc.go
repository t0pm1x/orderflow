// Package domain contains the Order aggregate and its state machine.
//
// States: pending → reserved → confirmed
//                     \-> cancelled
//                      \-> failed
//
// Implemented in sub-stage 3.4.b.
package domain