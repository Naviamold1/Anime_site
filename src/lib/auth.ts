import { betterAuth } from "better-auth";
import { drizzleAdapter } from "better-auth/adapters/drizzle";
import { db } from "@/db/client";
import * as schema from "@/db/schema";
import * as dotenv from "dotenv";

dotenv.config();

export const auth = betterAuth({
	database: drizzleAdapter(db, {
		provider: "sqlite",
		schema: {
			...schema,
		},
	}),
	user: {
		additionalFields: {
			onboarded: {
				type: "boolean",
				required: false,
				defaultValue: false,
			},
		},
	},
	socialProviders: {
		google: {
			clientId: process.env.GOOGLE_CLIENT_ID as string,
			clientSecret: process.env.GOOGLE_CLIENT_SECRET as string,
		},
	},
});
