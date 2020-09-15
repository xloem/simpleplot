#include <libshabal.h>

struct nonce
{
	struct scoop {
		uint8_t hash_a[32];
		// in POC2, hash_b is swapped with that of the mirror scoop index=4095-index
		uint8_t hash_b[32];
	};
	struct scoop scoops[4096];
};

// when mining, data is read by scoop, not by nonce.
// POC1 filename: AccountID_StartingNonce_NrOfNonces_Stagger [stagger is nonces per group]
// POC2 filename: AccountID_StartingNonce_NrOfNonces

int main()
{
}
