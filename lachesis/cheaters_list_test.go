package lachesis

import (
	"testing"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

func TestCheatersValidate(t *testing.T) {
	tests := []struct {
		name           string
		cheaters       Cheaters
		maxValidatorID idx.ValidatorID
		wantErr        bool
	}{
		{
			name:           "empty list is valid",
			cheaters:       Cheaters{},
			maxValidatorID: 10,
			wantErr:        false,
		},
		{
			name:           "valid single entry",
			cheaters:       Cheaters{1},
			maxValidatorID: 5,
			wantErr:        false,
		},
		{
			name:           "valid multiple distinct entries",
			cheaters:       Cheaters{1, 3, 5},
			maxValidatorID: 10,
			wantErr:        false,
		},
		{
			name:           "zero ID is rejected",
			cheaters:       Cheaters{0},
			maxValidatorID: 10,
			wantErr:        true,
		},
		{
			name:           "zero ID mixed with valid entries",
			cheaters:       Cheaters{1, 0, 3},
			maxValidatorID: 10,
			wantErr:        true,
		},
		{
			name:           "duplicate IDs are rejected",
			cheaters:       Cheaters{2, 2},
			maxValidatorID: 10,
			wantErr:        true,
		},
		{
			name:           "out-of-range ID is rejected",
			cheaters:       Cheaters{11},
			maxValidatorID: 10,
			wantErr:        true,
		},
		{
			name:           "maxValidatorID=0 skips upper-bound check",
			cheaters:       Cheaters{999},
			maxValidatorID: 0,
			wantErr:        false,
		},
		{
			name:           "ID equal to maxValidatorID is allowed",
			cheaters:       Cheaters{5},
			maxValidatorID: 5,
			wantErr:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cheaters.Validate(tc.maxValidatorID)
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
