/** Extension point for a future OIDC/provider identity integration. Authentication is not enabled by this challenge implementation. */
export abstract class ProviderIdentityPort {
  abstract assertAuthorized(
    providerId: string,
    credential: string | undefined,
  ): Promise<void>;
}
