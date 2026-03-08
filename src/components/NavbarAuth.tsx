import { signIn, signOut, useSession } from "@/lib/auth-client";

export default function NavbarAuth() {
	const { data: session, isPending } = useSession();

	if (isPending) {
		return <div className="h-8 w-24 animate-pulse rounded-md bg-zinc-800" />;
	}

	if (session) {
		return (
			<div className="flex items-center gap-3">
				<a href="/settings" className="group flex items-center gap-2">
					{session.user.image ? (
						<img
							src={session.user.image}
							alt={session.user.name}
							className="h-8 w-8 rounded-full border border-zinc-700 object-cover transition-colors group-hover:border-orange-500"
						/>
					) : (
						<div className="flex h-8 w-8 items-center justify-center rounded-full bg-zinc-800 text-xs font-bold text-zinc-300 transition-colors group-hover:bg-orange-500">
							{session.user.name.charAt(0).toUpperCase()}
						</div>
					)}
					<span className="hidden text-sm font-medium text-zinc-200 transition-colors group-hover:text-orange-400 sm:block">
						{session.user.name}
					</span>
				</a>
				<button
					onClick={() => signOut()}
					className="rounded-md px-3 py-1.5 text-xs font-semibold text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-white"
				>
					Sign Out
				</button>
			</div>
		);
	}

	return (
		<button
			onClick={() => signIn.social({ provider: "google" })}
			className="flex items-center gap-2 rounded-lg bg-white px-4 py-1.5 text-sm font-bold text-black transition-colors hover:bg-zinc-200"
		>
			<svg className="h-4 w-4" viewBox="0 0 24 24">
				<path
					fill="currentColor"
					d="M12.545,10.239v3.821h5.445c-0.712,2.315-2.647,3.972-5.445,3.972c-3.332,0-6.033-2.701-6.033-6.032s2.701-6.032,6.033-6.032c1.498,0,2.866,0.549,3.921,1.453l2.814-2.814C17.503,2.988,15.139,2,12.545,2C7.021,2,2.543,6.477,2.543,12s4.478,10,10.002,10c8.396,0,10.249-7.85,9.426-11.748L12.545,10.239z"
				/>
			</svg>
			Sign in
		</button>
	);
}
