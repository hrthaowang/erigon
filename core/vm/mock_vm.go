package vm

import (
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
)

type readonlyGetSetter interface {
	setReadonly(outerReadonly bool) func()
	getReadonly() bool
}

type testVM struct {
	readonlyGetSetter
	isTEMV bool

	recordedReadOnlies  *[]*readOnlyState
	recordedIsEVMCalled *[]bool

	env               *EVM
	isEVMSliceTest    []bool
	readOnlySliceTest []bool
	currentIdx        *int
}

func (evm *testVM) Run(_ *Contract, _ []byte, readOnly bool) (ret []byte, err error) {
	currentReadOnly := new(readOnlyState)

	currentReadOnly.outer = readOnly
	currentReadOnly.before = evm.getReadonly()

	currentIndex := *evm.currentIdx

	callback := evm.setReadonly(readOnly)
	defer func() {
		callback()
		currentReadOnly.after = evm.getReadonly()
	}()

	currentReadOnly.in = evm.getReadonly()

	(*evm.recordedReadOnlies)[currentIndex] = currentReadOnly
	(*evm.recordedIsEVMCalled)[currentIndex] = !evm.isTEMV

	*evm.currentIdx++

	if *evm.currentIdx < len(evm.readOnlySliceTest) {
		res, err := run(evm.env, NewContract(
			&dummyContractRef{},
			&dummyContractRef{},
			new(uint256.Int),
			0,
			false,
			!evm.isEVMSliceTest[*evm.currentIdx],
		), nil, evm.readOnlySliceTest[*evm.currentIdx])
		return res, err
	}

	return
}

type readOnlyState struct {
	outer  bool
	before bool
	in     bool
	after  bool
}

func (r *readOnlyState) String() string {
	return fmt.Sprintf("READONLY Status: outer %t; before %t; in %t; after %t", r.outer, r.before, r.in, r.after)
}

type dummyContractRef struct {
	calledForEach bool
}

func (dummyContractRef) ReturnGas(*big.Int)          {}
func (dummyContractRef) Address() common.Address     { return common.Address{} }
func (dummyContractRef) Value() *big.Int             { return new(big.Int) }
func (dummyContractRef) SetCode(common.Hash, []byte) {}
func (d *dummyContractRef) ForEachStorage(callback func(key, value common.Hash) bool) {
	d.calledForEach = true
}
func (d *dummyContractRef) SubBalance(amount *big.Int) {}
func (d *dummyContractRef) AddBalance(amount *big.Int) {}
func (d *dummyContractRef) SetBalance(*big.Int)        {}
func (d *dummyContractRef) SetNonce(uint64)            {}
func (d *dummyContractRef) Balance() *big.Int          { return new(big.Int) }

type dummyStatedb struct {
	state.IntraBlockState
}

func (*dummyStatedb) GetRefund() uint64 { return 1337 }
